"""Stream type classes for tap-googleads."""

from __future__ import annotations

import copy
import datetime
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_googleads.client import GoogleAdsStream, ResumableAPIError, _sanitise_customer_id
from pendulum import parse

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AccessibleCustomers(GoogleAdsStream):
    """Accessible Customers."""

    rest_method = "GET"
    path = "/customers:listAccessibleCustomers"
    name = "stream_accessible_customers"
    primary_keys = ["resourceNames"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("resourceNames", th.ArrayType(th.StringType)),
    ).to_dict()

    def get_child_context(
        self,
        record: Record,
        context,
    ):
        """Generate child contexts.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        Yields:
            A child context for each child stream.

        """
        customer_ids = []
        for customer in record.get("resourceNames", []):
            customer_id = customer.split("/")[1]
            customer_ids.append(customer_id.replace("-", ""))

        return {"customer_ids": customer_ids}

class CustomerHierarchyStream(GoogleAdsStream):
    """Customer Hierarchy.

    Inspiration from Google here
    https://developers.google.com/google-ads/api/docs/account-management/get-account-hierarchy.

    This query retrieves all 1-degree subaccounts given a manager account's subaccounts. Subaccounts can be either managers or clients.
    
    This stream spawns child streams only for customers that are active clients (not managers).
    If a `customer_ids` config is provided, only the customers in the list (or their children) will be synced.
    """

    @property
    def gaql(self):
        return """
	SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.status,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1
	"""

    records_jsonpath = "$.results[*]"
    name = "stream_customer_hierarchy"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = AccessibleCustomers
    state_partitioning_keys = ["customer_id"]
    schema = th.PropertiesList(
        th.Property("customer_id", th.StringType),
        th.Property("resourceName", th.StringType),
        th.Property("clientCustomer", th.StringType),
        th.Property("level", th.StringType),
        th.Property("status", th.StringType),
        th.Property("timeZone", th.StringType),
        th.Property("manager", th.BooleanType),
        th.Property("descriptiveName", th.StringType),
        th.Property("currencyCode", th.StringType),
        th.Property("id", th.StringType),
    ).to_dict()

    seen_customer_ids = set()

    def validate_response(self, response):
        if response.status_code == HTTPStatus.FORBIDDEN:
            msg = self.response_error_message(response)
            raise ResumableAPIError(msg, response)

        super().validate_response(response)


    def get_records(self, context):
        for customer_id in context.get("customer_ids", []):
            try:
                context["customer_id"] = customer_id
                yield from super().get_records(context)
            except Exception as e:
                self.logger.error(f"Error processing resource name {customer_id}: {str(e)}")
                continue

    def post_process(self, row, context):
        row = row["customerClient"]
        row["customer_id"] = _sanitise_customer_id(row["id"])
        return row
    
    def _sync_children(self, child_context: dict | None) -> None:
        if child_context:
            self.seen_customer_ids.add(child_context.get("customer_id"))
            super()._sync_children({"customer_id": child_context.get("customer_id")})

    def get_customer_family_line(self, resource_name) -> list:
        # resource name looks like 'customers/8435753557/customerClients/8105937676'
        family_line = [x for x in resource_name.split('/') if not 'customer' in x]
        return family_line

    def get_child_context(self, record: Record, context):
        customer_id = record.get("customer_id")
        is_active_client = record.get("manager") == False and record.get("status") == "ENABLED"
        already_synced = customer_id in self.seen_customer_ids

        family_line = self.get_customer_family_line(record.get("resourceName"))

        if is_active_client and not already_synced:
            if not self.customer_ids or len(set(self.customer_ids).intersection(set(family_line))) > 0:
                return {"customer_id": record.get("id"), "is_active_client": is_active_client}
        
        return None

class ReportsStream(GoogleAdsStream):
    parent_stream_type = CustomerHierarchyStream

    def get_records(self, context):
        records =  super().get_records(context)
        customer_id = context.get("customer_id")
        if customer_id:
            for record in records:
                record["customer_id"] = customer_id
                yield record

class GeotargetsStream(ReportsStream):
    """Geotargets, worldwide, constant across all customers"""

    gaql = """
    SELECT
        geo_target_constant.canonical_name,
        geo_target_constant.country_code,
        geo_target_constant.id,
        geo_target_constant.name,
        geo_target_constant.status,
        geo_target_constant.target_type
    FROM geo_target_constant
    """
    records_jsonpath = "$.results[*]"
    name = "stream_geo_target_constant"
    primary_keys = ["geoTargetConstant__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "geo_target_constant.json"

    def get_records(self, context: Context) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.

        """
        yield from super().get_records(context)
        self.selected = False  # sync once only

class ClickViewReportStream(ReportsStream):
    date: datetime.date

    @property
    def gaql(self):
        return f"""
        SELECT
            click_view.gclid
            , customer.id
            , click_view.ad_group_ad
            , ad_group.id
            , ad_group.name
            , campaign.id
            , campaign.name
            , segments.ad_network_type
            , segments.device
            , segments.date
            , segments.slot
            , metrics.clicks
            , segments.click_type
            , click_view.keyword
            , click_view.keyword_info.match_type
        FROM click_view
        WHERE segments.date = '{self.date.isoformat()}'
        """

    records_jsonpath = "$.results[*]"
    name = "stream_click_view_report"
    primary_keys = [
        "clickView__gclid",
        "clickView__keyword",
        "clickView__keywordInfo__matchType",
        "customer__id",
        "adGroup__id",
        "campaign__id",
        "segments__device",
        "segments__adNetworkType",
        "segments__slot",
        "date",
    ]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "click_view_report.json"

    def post_process(self, row, context):
        row["date"] = row["segments"].pop("date")

        if row.get("clickView", {}).get("keyword") is None:
            row["clickView"]["keyword"] = "null"
            row["clickView"]["keywordInfo"] = {"matchType": "null"}

        return row

    def get_url_params(self, context, next_page_token):
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.

        """
        params: dict = {}
        if next_page_token:
            params["pageToken"] = next_page_token
        return params

    def request_records(self, context):
        start_value = self.get_starting_replication_key_value(context)

        start_date =  parse(start_value).date()
        end_date = parse(self.config["end_date"]).date()

        delta = end_date - start_date
        dates = (start_date + datetime.timedelta(days=i) for i in range(delta.days))

        for self.date in dates:
            records = list(super().request_records(context))

            if not records:
                self._increment_stream_state({"date": self.date.isoformat()}, context=self.context)

            yield from records

    def validate_response(self, response):
        if response.status_code == HTTPStatus.FORBIDDEN:
            error = response.json()["error"]["details"][0]["errors"][0]
            msg = (
                "Click view report not accessible to customer "
                f"'{self.context['customer_id']}': {error['message']}"
            )
            raise ResumableAPIError(msg, response)

        super().validate_response(response)

class AccountsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return f"""
            select
              customer.id,
              customer.auto_tagging_enabled,
              customer.currency_code,
              customer.descriptive_name,
              customer.final_url_suffix,
              customer.manager,
              customer.optimization_score,
              customer.pay_per_conversion_eligibility_failure_reasons,
              customer.test_account,
              customer.time_zone,
              customer.tracking_url_template,
              customer.status
            from
              customer
        """

    records_jsonpath = "$.results[*]"
    name = "stream_account"
    primary_keys = ["customer__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "account.json"

class AdPerformanceReportConversionStats(ReportsStream):
    """Ad Performance Report Conversion Stats stream."""
    
    @property
    def gaql(self) -> str:
        return f"""
        SELECT 
            customer.id,
            ad_group_ad.ad.id,
            ad_group.id,
            campaign.id,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.conversions,
            metrics.conversions_value,
            segments.conversion_action_name,
            segments.device,
            segments.date
        FROM ad_group_ad
        WHERE segments.date >= {self.start_date}
        AND segments.date <= {self.end_date}
        """

    name = "stream_ad_performance_report_conversion_stats"
    records_jsonpath = "$.results[*]"
    primary_keys = [
        "customer__id",
        "ad_group_ad__ad__id",
        "ad_group__id",  # Added ad_group_id as primary key
        "segments__date",
        "segments__conversion_action_name",
        "segments__device"
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_performance_report_conversion_stats.json"

class CampaignsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          campaign.id,
          campaign.ad_serving_optimization_status,
          campaign.advertising_channel_type,
          campaign.advertising_channel_sub_type,
          campaign.base_campaign,
          campaign.end_date,
          campaign.experiment_type,
          campaign.final_url_suffix,
          campaign.frequency_caps,
          campaign.name,
          campaign.optimization_score,
          campaign.payment_mode,
          campaign.serving_status,
          campaign.start_date,
          campaign.status,
          campaign.tracking_url_template,
          campaign.vanity_pharma.vanity_pharma_display_url_mode,
          campaign.vanity_pharma.vanity_pharma_text,
          campaign.video_brand_safety_suitability,
          campaign.labels
        from
          campaign
        """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign"
    primary_keys = ["campaign__id", "customer__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign.json"

class CampaignReportsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return f"""
            select
                customer.id,
                campaign.id,
                campaign.name,
                metrics.absolute_top_impression_percentage,
                metrics.all_conversions,
                metrics.clicks,
                metrics.conversions,
                metrics.conversions_value,
                metrics.cost_micros,
                metrics.impressions,
                metrics.phone_calls,
                metrics.search_budget_lost_absolute_top_impression_share,
                metrics.search_budget_lost_impression_share,
                metrics.search_budget_lost_top_impression_share,
                metrics.search_absolute_top_impression_share,
                metrics.search_exact_match_impression_share,
                metrics.search_impression_share,
                metrics.search_rank_lost_absolute_top_impression_share,
                metrics.search_rank_lost_impression_share,
                metrics.search_rank_lost_top_impression_share,
                metrics.search_top_impression_share,
                metrics.top_impression_percentage,
                segments.date
            from
                campaign
            WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_report"
    primary_keys = ["campaign__id", "segments__date", "customer__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_report.json"

class CampaignReportCustomConversionsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return f"""
            SELECT
                customer.id,
                campaign.id,
                campaign.name,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.conversions,
                metrics.conversions_value,
                segments.conversion_action_name,
                segments.date
            from
                campaign
            WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_report_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "segments__conversionActionName", "segments__date"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_report_custom_conversions.json"

class CityReportStream(ReportsStream):
    """Define custom stream for city-level reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          geographic_view.country_criterion_id,
          geographic_view.location_type,
          geographic_view.resource_name,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          segments.geo_target_city,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_city_report"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "segments__date", "segments__geoTargetCity", "geographicView__locationType"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "city_report.json"

class CityReportCustomConversionsStream(ReportsStream):
    """Define custom stream for city-level conversion reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          geographic_view.country_criterion_id,
          geographic_view.location_type,
          geographic_view.resource_name,
          metrics.all_conversions,
          metrics.all_conversions_value,
          metrics.conversions,
          metrics.conversions_value,
          segments.conversion_action_name,
          segments.geo_target_city,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_city_report_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "segments__date", "segments__conversionActionName", "segments__geoTargetCity", "geographicView__locationType"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "city_report_custom_conversions.json"

class CountryReportStream(ReportsStream):
    """Define country report stream."""

    @property
    def gaql(self):
        return f"""
            select
                customer.id,
                campaign.id,
                geographic_view.country_criterion_id,
                geographic_view.location_type,
                metrics.clicks,
                metrics.conversions,
                metrics.conversions_value,
                metrics.impressions,
                metrics.cost_micros,
                segments.date
            from
                geographic_view
            WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_country_report"
    primary_keys = ["customer__id", "campaign__id", "geographicView__countryCriterionId", "segments__date", "geographicView__locationType"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "country_report.json"

class CountryReportCustomConversionsStream(ReportsStream):
    """Define country report custom conversions stream."""

    @property
    def gaql(self):
        return f"""
            select
                customer.id,
                campaign.id,
                geographic_view.country_criterion_id,
                geographic_view.location_type,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.conversions,
                metrics.conversions_value,
                segments.conversion_action_name,
                segments.date
            from
                geographic_view
            WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_country_report_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "geographicView__countryCriterionId", "segments__conversionActionName", "segments__date", "geographicView__locationType"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "country_report_custom_conversions.json"

class DemoDeviceStream(ReportsStream):
    """Define custom stream for device-level campaign reporting."""

    @property
    def gaql(self):
        return f"""
        SELECT
          customer.id,
          campaign.id,
          segments.date,
          segments.device,
          metrics.all_conversions,
          metrics.all_conversions_value,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          metrics.view_through_conversions
        FROM
          campaign
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_demo_device"
    primary_keys = ["customer__id", "campaign__id", "segments__date", "segments__device"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "demo_device.json"

class DemoDeviceCustomConversionsStream(ReportsStream):
    """Define custom stream for device-level campaign conversion reporting."""

    @property
    def gaql(self):
        return f"""
        SELECT
          customer.id,
          campaign.id,
          segments.date,
          segments.device,
          segments.conversion_action_name,
          metrics.all_conversions,
          metrics.all_conversions_value,
          metrics.conversions,
          metrics.conversions_value,
          metrics.view_through_conversions
        FROM
          campaign
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_demo_device_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "segments__date", "segments__device", "segments__conversionActionName"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "demo_device_custom_conversions.json"

class DemoRegionStream(ReportsStream):
    """Define custom stream for region-level reporting."""

    @property
    def gaql(self):
        return f"""
        SELECT
          geographic_view.country_criterion_id,
          segments.date,
          segments.geo_target_region,
          metrics.all_conversions,
          metrics.all_conversions_value,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          metrics.view_through_conversions,
          customer.id,
          campaign.id
        FROM
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_demo_region"
    primary_keys = ["customer__id", "campaign__id", "segments__date", "geographicView__countryCriterionId", "segments__geoTargetRegion"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "demo_region.json"

class DemoRegionCustomConversionsStream(ReportsStream):
    """Define custom stream for region-level conversion reporting."""

    @property
    def gaql(self):
        return f"""
        SELECT
          geographic_view.country_criterion_id,
          segments.date,
          segments.geo_target_region,
          segments.conversion_action_name,
          metrics.all_conversions,
          metrics.all_conversions_value,
          metrics.conversions,
          metrics.conversions_value,
          metrics.view_through_conversions,
          customer.id,
          campaign.id
        FROM
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_demo_region_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "segments__date", "geographicView__countryCriterionId", "segments__conversionActionName", "segments__geoTargetRegion"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "demo_region_custom_conversions.json"

class ExpandedTextAdStream(ReportsStream):
    """Define custom stream for expanded text ads."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          campaign.id,
          ad_group.id,
          ad_group_ad.ad.id,
          ad_group_ad.ad.expanded_text_ad.description,
          ad_group_ad.ad.expanded_text_ad.description2,
          ad_group_ad.ad.expanded_text_ad.headline_part1,
          ad_group_ad.ad.expanded_text_ad.headline_part2,
          ad_group_ad.ad.expanded_text_ad.headline_part3,
          ad_group_ad.ad.expanded_text_ad.path1,
          ad_group_ad.ad.expanded_text_ad.path2
        from
          ad_group_ad
        """

    records_jsonpath = "$.results[*]"
    name = "stream_expanded_text_ad"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "adGroupAd__ad__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "expanded_text_ad.json"

class GenderReportStream(ReportsStream):
    """Define custom stream for gender-level reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          ad_group_criterion.criterion_id,
          ad_group_criterion.gender.type,
          campaign.id,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          segments.date
        from
          gender_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_gender_report"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "adGroupCriterion__criterionId", "segments__date", "adGroupCriterion__gender__type"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "gender_report.json"

class GenderReportCustomConversionsStream(ReportsStream):
    """Define custom stream for gender-level conversion reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          ad_group_criterion.criterion_id,
          ad_group_criterion.gender.type,
          campaign.id,
          metrics.all_conversions,
          metrics.all_conversions_value,
          metrics.conversions,
          metrics.conversions_value,
          segments.conversion_action_name,
          segments.date
        from
          gender_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_gender_report_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "adGroupCriterion__criterionId", "segments__date", "segments__conversionActionName", "adGroupCriterion__gender__type"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "gender_report_custom_conversions.json"

class MetroReportStream(ReportsStream):
    """Define custom stream for metro-level reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          geographic_view.resource_name,
          geographic_view.country_criterion_id,
          geographic_view.location_type,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          metrics.all_conversions,
          metrics.all_conversions_value,
          segments.geo_target_metro,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_metro_report"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "geographicView__countryCriterionId", "segments__date", "geographicView__locationType", "segments__geoTargetMetro"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "metro_report.json"

class MetroReportCustomConversionsStream(ReportsStream):
    """Define custom stream for metro-level conversion reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          geographic_view.resource_name,
          geographic_view.country_criterion_id,
          geographic_view.location_type,
          metrics.conversions,
          metrics.conversions_value,
          metrics.all_conversions,
          metrics.all_conversions_value,
          segments.conversion_action_name,
          segments.geo_target_metro,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_metro_report_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "geographicView__countryCriterionId", "segments__date", "segments__conversionActionName", "geographicView__locationType", "segments__geoTargetMetro"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "metro_report_custom_conversions.json"

class PostalCodeReportStream(ReportsStream):
    """Define custom stream for postal code-level reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          geographic_view.resource_name,
          geographic_view.country_criterion_id,
          geographic_view.location_type,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          metrics.all_conversions,
          metrics.all_conversions_value,
          segments.geo_target_city,
          segments.geo_target_postal_code,
          segments.geo_target_state,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_postal_code_report"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "geographicView__countryCriterionId", "segments__date", "geographicView__locationType", "segments__geoTargetCity", "segments__geoTargetPostalCode", "segments__geoTargetState"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "postal_code_report.json"

class PostalCodeReportCustomConversionsStream(ReportsStream):
    """Define custom stream for postal code-level conversion reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          geographic_view.resource_name,
          geographic_view.country_criterion_id,
          geographic_view.location_type,
          metrics.conversions,
          metrics.conversions_value,
          metrics.all_conversions,
          metrics.all_conversions_value,
          segments.geo_target_city,
          segments.geo_target_postal_code,
          segments.geo_target_state,
          segments.conversion_action_name,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_postal_code_report_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "geographicView__countryCriterionId", "segments__date", "segments__conversionActionName", "geographicView__locationType", "segments__geoTargetCity", "segments__geoTargetPostalCode", "segments__geoTargetState"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "postal_code_report_custom_conversions.json"

class PostalCodeReportCampaignLevelStream(ReportsStream):
    """Define custom stream for campaign-level postal code reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          campaign.id,
          geographic_view.resource_name,
          geographic_view.location_type,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          metrics.all_conversions,
          metrics.all_conversions_value,
          segments.geo_target_postal_code,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_postal_code_report_campaign_level"
    primary_keys = ["customer__id", "campaign__id", "segments__date", "geographicView__locationType", "segments__geoTargetPostalCode"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "postal_code_report_campaign_level.json"

class PostalCodeReportCampaignLevelCustomConversionsStream(ReportsStream):
    """Define custom stream for campaign-level postal code conversion reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          campaign.id,
          geographic_view.resource_name,
          geographic_view.location_type,
          metrics.conversions,
          metrics.conversions_value,
          metrics.all_conversions,
          metrics.all_conversions_value,
          segments.geo_target_postal_code,
          segments.conversion_action_name,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_postal_code_report_campaign_level_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "segments__date", "segments__conversionActionName", "geographicView__locationType", "segments__geoTargetPostalCode"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "postal_code_report_campaign_level_custom_conversions.json"

class RegionReportStream(ReportsStream):
    """Define custom stream for region-level reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          geographic_view.country_criterion_id,
          geographic_view.location_type,
          geographic_view.resource_name,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          segments.geo_target_region,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_region_report"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "geographicView__countryCriterionId", "segments__date", "geographicView__locationType", "segments__geoTargetRegion"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "region_report.json"

class RegionReportCustomConversionsStream(ReportsStream):
    """Define custom stream for region-level conversion reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          geographic_view.country_criterion_id,
          geographic_view.location_type,
          geographic_view.resource_name,
          metrics.all_conversions,
          metrics.all_conversions_value,
          metrics.conversions,
          metrics.conversions_value,
          segments.conversion_action_name,
          segments.geo_target_region,
          segments.date
        from
          geographic_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_region_report_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "geographicView__countryCriterionId", "segments__date", "segments__conversionActionName", "geographicView__locationType", "segments__geoTargetRegion"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "region_report_custom_conversions.json"

class ResponsiveSearchAdStream(ReportsStream):
    """Define custom stream for responsive search ads."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          ad_group_ad.ad.id,
          ad_group_ad.ad.responsive_search_ad.descriptions,
          ad_group_ad.ad.responsive_search_ad.headlines,
          ad_group_ad.ad.responsive_search_ad.path1,
          ad_group_ad.ad.responsive_search_ad.path2
        from
          ad_group_ad
        """

    records_jsonpath = "$.results[*]"
    name = "stream_responsive_search_ad"
    primary_keys = ["customer__id", "adGroup__id", "adGroupAd__ad__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "responsive_search_ad.json"

class SearchQueryReportStream(ReportsStream):
    """Define custom stream for search query reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          search_term_view.search_term,
          segments.keyword.ad_group_criterion,
          segments.keyword.info.match_type,
          segments.keyword.info.text,
          segments.search_term_match_type,
          segments.date
        from
          search_term_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_search_query_report"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "searchTermView__searchTerm", "segments__date", "segments__keyword__adGroupCriterion", "segments__keyword__info", "segments__searchTermMatchType"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "search_query_report.json"

class SearchQueryReportCustomConversionsStream(ReportsStream):
    """Define custom stream for search query conversion reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          ad_group.id,
          campaign.id,
          metrics.all_conversions,
          metrics.all_conversions_value,
          metrics.conversions,
          metrics.conversions_value,
          search_term_view.search_term,
          segments.conversion_action_name,
          segments.keyword.ad_group_criterion,
          segments.search_term_match_type,
          segments.date
        from
          search_term_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_search_query_report_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "searchTermView__searchTerm", "segments__date", "segments__conversionActionName", "segments__keyword__adGroupCriterion", "segments__searchTermMatchType"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "search_query_report_custom_conversions.json"

class VideoStream(ReportsStream):
    """Define custom stream for video reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          video.id,
          video.channel_id,
          video.duration_millis,
          video.resource_name,
          video.title
        from
          video
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_video"
    primary_keys = ["customer__id", "video__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "video.json"

class VideoReportStream(ReportsStream):
    """Define custom stream for video performance reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          campaign.id,
          ad_group.id,
          video.id,
          metrics.clicks,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros,
          metrics.impressions,
          metrics.all_conversions,
          metrics.all_conversions_value,
          metrics.video_quartile_p100_rate,
          metrics.video_quartile_p75_rate,
          metrics.video_quartile_p50_rate,
          metrics.video_quartile_p25_rate,
          metrics.video_view_rate,
          metrics.video_views,
          segments.date
        from
          video
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_video_report"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "video__id", "segments__date"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "video_report.json"

class VideoReportCustomConversionsStream(ReportsStream):
    """Define custom stream for video conversion reporting."""

    @property
    def gaql(self):
        return f"""
        select
          customer.id,
          campaign.id,
          ad_group.id,
          video.id,
          metrics.conversions,
          metrics.conversions_value,
          metrics.all_conversions,
          metrics.all_conversions_value,
          segments.conversion_action_name,
          segments.date
        from
          video
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_video_report_custom_conversions"
    primary_keys = ["customer__id", "campaign__id", "adGroup__id", "video__id", "segments__date", "segments__conversionActionName"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "video_report_custom_conversions.json"

class KeywordReportsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return f"""
            select
                customer.id,
                ad_group.id,
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.keyword.text,
                ad_group_criterion.labels,
                ad_group_criterion.position_estimates.first_page_cpc_micros,
                ad_group_criterion.position_estimates.first_position_cpc_micros,
                ad_group_criterion.status,
                campaign.id,
                metrics.absolute_top_impression_percentage,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.clicks,
                metrics.conversions,
                metrics.conversions_value,
                metrics.cost_micros,
                metrics.historical_creative_quality_score,
                metrics.historical_landing_page_quality_score,
                metrics.historical_quality_score,
                metrics.historical_search_predicted_ctr,
                metrics.impressions,
                metrics.search_absolute_top_impression_share,
                metrics.search_budget_lost_absolute_top_impression_share,
                metrics.search_budget_lost_top_impression_share,
                metrics.search_impression_share,
                metrics.search_rank_lost_absolute_top_impression_share,
                metrics.search_rank_lost_impression_share,
                metrics.search_rank_lost_top_impression_share,
                metrics.search_top_impression_share,
                metrics.top_impression_percentage,
                metrics.view_through_conversions,
                segments.date
            from
                keyword_view
            WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_keyword_report"
    primary_keys = ["customer__id", "adGroup__id", "adGroupCriterion__criterionId", "campaign__id", "segments__date"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "keyword_report.json"

class KeywordReportCustomConversionsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return f"""
            select
                customer.id,
                ad_group.id,
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.keyword.text,
                campaign.id,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.conversions,
                metrics.conversions_value,
                segments.conversion_action_name,
                segments.date
            from
                keyword_view
            WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_keyword_report_custom_conversions"
    primary_keys = ["customer__id", "adGroup__id", "adGroupCriterion__criterionId", "campaign__id", "segments__conversionActionName", "segments__date"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "keyword_report_custom_conversions.json"

class AdStream(ReportsStream):
    """Stream for basic ad information from Google Ads."""
    
    @property
    def gaql(self):
        query = """
        SELECT
            customer.id,
            ad_group_ad.ad.id,
            ad_group.id,
            ad_group_ad.ad.display_url,
            ad_group_ad.ad.final_app_urls,
            ad_group_ad.ad.final_mobile_urls,
            ad_group_ad.ad.final_url_suffix,
            ad_group_ad.ad.final_urls,
            ad_group_ad.ad.name,
            ad_group_ad.status,
            ad_group_ad.ad.tracking_url_template,
            ad_group_ad.ad.type
        FROM
            ad_group_ad
        """
        return query

    records_jsonpath = "$.results[*]"
    name = "stream_ad"
    primary_keys = ["customer__id", "adGroupAd__ad__id", "adGroup__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad.json"

class AdReportStream(ReportsStream):
    """Stream for ad report information from Google Ads."""

    @property
    def gaql(self):
        return f"""
        SELECT
            customer.id,
            ad_group.id,
            ad_group.name,
            ad_group.status,
            ad_group_ad.ad.added_by_google_ads,
            ad_group_ad.ad.call_ad.description1,
            ad_group_ad.ad.call_ad.description2,
            ad_group_ad.ad.device_preference,
            ad_group_ad.ad.display_url,
            ad_group_ad.ad.expanded_text_ad.description,
            ad_group_ad.ad.expanded_text_ad.description2,
            ad_group_ad.ad.expanded_text_ad.headline_part1,
            ad_group_ad.ad.expanded_text_ad.headline_part2,
            ad_group_ad.ad.expanded_text_ad.headline_part3,
            ad_group_ad.ad.expanded_text_ad.path1,
            ad_group_ad.ad.expanded_text_ad.path2,
            ad_group_ad.ad.final_mobile_urls,
            ad_group_ad.ad.final_urls,
            ad_group_ad.ad.id,
            ad_group_ad.ad.legacy_responsive_display_ad.call_to_action_text,
            ad_group_ad.ad.legacy_responsive_display_ad.description,
            ad_group_ad.ad.text_ad.description1,
            ad_group_ad.ad.text_ad.description2,
            ad_group_ad.ad.text_ad.headline,
            ad_group_ad.ad.tracking_url_template,
            ad_group_ad.ad.type,
            ad_group_ad.ad.url_custom_parameters,
            ad_group_ad.labels,
            ad_group_ad.policy_summary.approval_status,
            ad_group_ad.status,
            campaign.id,
            campaign.name,
            campaign.status,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value,
            metrics.cost_micros,
            metrics.impressions,
            metrics.view_through_conversions,
            segments.date
        FROM
            ad_group_ad
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """
    records_jsonpath = "$.results[*]"
    name = "stream_ad_report"
    primary_keys = ["customer__id", "adGroupAd__ad__id", "adGroup__id", "segments__date"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_report.json"

class AdStatsStream(ReportsStream):
    """Stream for ad stats information from Google Ads."""

    @property
    def gaql(self):
        return f"""
            SELECT
                customer.id,
                ad_group.id,
                ad_group_ad.ad.id,
                campaign.id,
                metrics.active_view_impressions,
                metrics.active_view_measurability,
                metrics.active_view_measurable_cost_micros,
                metrics.active_view_measurable_impressions,
                metrics.active_view_viewability,
                metrics.clicks,
                metrics.conversions_value,
                metrics.conversions,
                metrics.cost_micros,
                metrics.impressions,
                metrics.interaction_event_types,
                metrics.interactions,
                metrics.view_through_conversions,
                segments.ad_network_type,
                segments.device,
                segments.date
            FROM
                ad_group_ad
            WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """
    records_jsonpath = "$.results[*]"
    name = "stream_ad_stats"
    primary_keys = ["customer__id", "adGroupAd__ad__id", "adGroup__id", "segments__date", "segments__device", "segments__adNetworkType"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_stats.json"

class AdGroupsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
       SELECT 
        customer.id,
        campaign.id,
        ad_group.url_custom_parameters,
        ad_group.type,
        ad_group.tracking_url_template,
        ad_group.targeting_setting.target_restrictions,
        ad_group.target_roas,
        ad_group.target_cpm_micros,
        ad_group.status,
        ad_group.target_cpa_micros,
        ad_group.resource_name,
        ad_group.percent_cpc_bid_micros,
        ad_group.name,
        ad_group.labels,
        ad_group.id,
        ad_group.final_url_suffix,
        ad_group.excluded_parent_asset_field_types,
        ad_group.effective_target_roas_source,
        ad_group.effective_target_roas,
        ad_group.effective_target_cpa_source,
        ad_group.effective_target_cpa_micros,
        ad_group.display_custom_bid_dimension,
        ad_group.cpv_bid_micros,
        ad_group.cpm_bid_micros,
        ad_group.cpc_bid_micros,
        ad_group.campaign,
        ad_group.base_ad_group,
        ad_group.ad_rotation_mode
       FROM ad_group
       """

    records_jsonpath = "$.results[*]"
    name = "stream_adgroups"
    primary_keys = ["adGroup__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_group.json"

class AdGroupsPerformance(ReportsStream):
    """AdGroups Performance"""

    @property
    def gaql(self):
        return f"""
        SELECT campaign.id, ad_group.id, metrics.impressions, metrics.clicks,
               metrics.cost_micros
               FROM ad_group
               WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_adgroupsperformance"
    primary_keys = ["campaign__id", "adGroup__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "adgroups_performance.json"

class AgeReportStream(ReportsStream):
    """Age Report"""

    @property
    def gaql(self):
        return f"""
        SELECT
            customer.id,
            ad_group.id,
            ad_group_criterion.age_range.type,
            ad_group_criterion.criterion_id,
            campaign.id,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value,
            metrics.impressions,
            metrics.cost_micros,
            segments.date
        FROM
            age_range_view
        WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_age_report"
    primary_keys = ["customer__id", "adGroup__id", "adGroupCriterion__ageRange__type", "campaign__id", "segments__date", "adGroupCriterion__criterionId"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "age_report.json"

class AgeReportCustomConversionsStream(ReportsStream):
    """Age Report Custom Conversions"""

    @property
    def gaql(self):
        return f"""
            select
                customer.id,
                ad_group.id,
                ad_group_criterion.age_range.type,
                ad_group_criterion.criterion_id,
                campaign.id,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.conversions,
                metrics.conversions_value,
                segments.conversion_action_name,
                segments.date
            FROM
                age_range_view
            WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_age_report_custom_conversions"
    primary_keys = ["customer__id", "adGroup__id", "adGroupCriterion__ageRange__type", "campaign__id", "segments__date", "adGroupCriterion__criterionId", "segments__conversionActionName"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "age_report_custom_conversions.json"
        
class CampaignPerformance(ReportsStream):
    """Campaign Performance"""

    @property
    def gaql(self):
        return f"""
    SELECT campaign.name, campaign.status, segments.device, segments.date, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM campaign WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance"
    primary_keys = [
        "campaign__name",
        "campaign__status",
        "segments__date",
        "segments__device",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance.json"

class CampaignPerformanceByAgeRangeAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""
    SELECT ad_group_criterion.age_range.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM age_range_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance_by_age_range_and_device"
    primary_keys = [
        "adGroup__name",
        "adGroupCriterion__ageRange__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_age_range_and_device.json"

class CampaignPerformanceByGenderAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""
    SELECT ad_group_criterion.gender.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM gender_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance_by_gender_and_device"
    primary_keys = [
        "adGroup__name",
        "adGroupCriterion__gender__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_gender_and_device.json"

class CampaignPerformanceByLocation(ReportsStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""
    SELECT campaign_criterion.location.geo_target_constant, campaign.name, campaign_criterion.bid_modifier, segments.date, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM location_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date} AND campaign_criterion.status != 'REMOVED'
    """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance_by_location"
    primary_keys = [
        "campaignCriterion__location__geoTargetConstant",
        "campaign__name",
        "segments__date",
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_location.json"

class GeoPerformance(ReportsStream):
    """Geo performance"""

    @property
    def gaql(self):
        return f"""
    SELECT
        campaign.name,
        campaign.status,
        segments.date,
        metrics.clicks,
        metrics.cost_micros,
        metrics.impressions,
        metrics.conversions,
        geographic_view.location_type,
        geographic_view.country_criterion_id
    FROM geographic_view
    WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    records_jsonpath = "$.results[*]"
    name = "stream_geo_performance"
    primary_keys = [
        "geographicView__countryCriterionId",
        "geographicView__locationType",
        "customer_id",
        "campaign__name",
        "campaign__status",
        "segments__date"
    ]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "geo_performance.json"
