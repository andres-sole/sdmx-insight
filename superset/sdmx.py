from sqlalchemy import create_engine
import uuid
import datetime
import pandas as pd
from sdmxthon.webservices import webservices
from sdmxthon.api.api import get_supported_agencies
from sdmxthon import read_sdmx
from superset.connectors.sqla.models import SqlaTable
from superset.extensions import security_manager
from superset.models.slice import Slice
from superset.dashboards.commands.create import CreateDashboardCommand
from superset.databases.commands.create import CreateDatabaseCommand
from superset import db
import json
import re
import sqlite3


def load_database(sdmx_url, dataset_instance=None, is_raw_url=False):
    # Extract main data and identifiers
    message = read_sdmx(sdmx_url)
    agency_id, dataflow_id = get_identifiers(sdmx_url)
    data = extract_data_from_message(message)

    concepts_name = {}
    if not is_raw_url:
        # Get the required web service
        ws = get_webservice_for_agency(agency_id)

        # Fetch metadata
        metadata = fetch_metadata(ws, dataflow_id)

        # Generate the final dataframe and concepts names
        df, concepts_name = generate_final_df_and_concepts_name(data, metadata.payload)
    else:
        df = data

    # Process the database
    dataset_uuid, database = process_database(dataset_instance, df, message, sdmx_url)

    # Update table permissions and metadata
    update_permissions_and_metadata(
        database, dataset_instance, dataset_uuid, sdmx_url, concepts_name
    )

    return table_instance


def get_identifiers(sdmx_url):
    """Retrieve agency and dataflow IDs from the SDMX URL."""
    agency_id = get_agency_id(sdmx_url)
    dataflow_id = get_dataflow_id(sdmx_url)
    return agency_id, dataflow_id


def extract_data_from_message(message):
    """Extract data from the message payload."""
    return list(message.payload.values())[0].data


def get_webservice_for_agency(agency_id):
    """Return the required web service for a given agency ID."""
    supported_agencies = get_supported_agencies()
    if agency_id not in supported_agencies:
        raise Exception("Agency not supported")
    return supported_agencies[agency_id]()


def fetch_metadata(ws, dataflow_id):
    """Retrieve metadata for a given dataflow ID."""
    try:
        return ws.get_data_flow(dataflow_id, references="descendants")
    except Exception as e:
        raise Exception(e, dataflow_id)


def process_database(dataset_instance, df, message, sdmx_url):
    """Create or update the database and populate it with data."""
    if dataset_instance is None:
        dataset_uuid = uuid.uuid4()
        database = CreateDatabaseCommand(
            {
                "sqlalchemy_uri": f"sqlite:///dbs/{dataset_uuid}",
                "database_name": f"{dataset_uuid}",
            }
        ).run()
    else:
        dataset_uuid = dataset_instance.sdmx_uuid
        database = dataset_instance.database

    engine = create_engine(f"sqlite:///dbs/{dataset_uuid}", echo=False)
    for dataset in message.payload.keys():
        df.to_sql(str(dataset) + " " + str(datetime.datetime.now()), con=engine)
    return dataset_uuid, database


def update_permissions_and_metadata(
    database, dataset_instance, dataset_uuid, sdmx_url, concepts_name={}
):
    """Update table permissions and metadata."""
    schemas = database.get_all_schema_names(cache=False)
    tables = database.get_all_table_names_in_schema(schemas[0], force=True)

    for schema in schemas:
        security_manager.add_permission_view_menu(
            "schema_access", security_manager.get_schema_perm(database, schema)
        )

    global table_instance
    for table in tables:
        if dataset_instance is None:
            table_instance = SqlaTable(
                table_name=f"{table[0]}",
                database=database,
                schema="main",
                is_sdmx=True,
                sdmx_url=sdmx_url,
                sdmx_uuid=dataset_uuid,
                concepts=json.dumps(concepts_name),
            )
        else:
            table_instance = dataset_instance
            table_instance.table_name = f"{table[0]}"
            db.session.add(table_instance)
            db.session.commit()

        table_instance.fetch_metadata()


def create_dashboard(spec):
    """Create a new dashboard based on the provided specification.

    Args:
        spec (dict): Specification containing dashboard details.
                     Expected to have a 'DashID' key.

    Returns:
        Dashboard object: Returns the created dashboard object.
    """
    dashboard_title = spec.get("DashID")
    if not dashboard_title:
        raise ValueError("Missing 'DashID' in the specification.")

    dashboard = CreateDashboardCommand({"dashboard_title": dashboard_title}).run()
    return dashboard


def get_locale_column_if_exists(data, column, locale):
    """Retrieve the locale-specific column if it exists.
    Fallback to the original column if locale-specific column doesn't exist.

    Args:
        data (dict or DataFrame): Data containing columns.
                                  Expected to be either a dictionary with a 'columns' key
                                  or a DataFrame.
        column (str): Original column name.
        locale (str): Desired locale (e.g., 'en', 'es', 'fr').

    Returns:
        str: Locale-specific column name or original column name if locale-specific
             column doesn't exist.

    Raises:
        Exception: If provided locale is not supported.
    """
    # Validate supported locales
    supported_locales = ["en", "es", "fr"]
    if locale not in supported_locales:
        raise Exception("Locale not supported")

    # Construct locale-specific column name
    locale_column = column + f"-{locale}"

    # Check for the locale-specific column based on the type of 'data'
    if isinstance(data, dict) and "columns" in data:
        for column_data in data["columns"]:
            if column_data["column_name"] == locale_column:
                return locale_column
    elif hasattr(data, "columns"):
        if locale_column in data.columns:
            return locale_column

    # If locale-specific column doesn't exist, return the original column name
    return column


def get_locale_value(df, column, locale):
    """Retrieve the locale-specific value from the first row of a DataFrame."""
    if locale not in ["en", "es", "fr"]:
        raise Exception("Locale not supported")
    return df[get_locale_column_if_exists(df, column, locale)].iloc[0]


def substitute_string(string, df, locale="en"):
    """Replace placeholders in the string with values from the DataFrame."""
    pattern = r"\{\$([^\}]+)\}"
    columns_to_substitute = re.findall(pattern, str(string))

    for column in columns_to_substitute:
        string = string.replace(
            f"{{${column}}}", str(get_locale_value(df, column, locale))
        )
    return string


def create_charts(spec, datasets, dashboard, locale="en"):
    """Create charts based on the given specification and datasets, and add to the dashboard."""

    for idx, row in enumerate(spec["Rows"]):
        if not row["DATA"]:
            continue

        # Fetch data from SQLite database
        with sqlite3.connect(f"dbs/{datasets[idx].sdmx_uuid}") as conn:
            table_name = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchone()[0]
            df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)

        # Columns always present
        mentioned_columns = ["OBS_VALUE"]
        pattern = r"\{\$([^\}]+)\}"

        # Substitute placeholders in the row with actual values
        for key in list(row.keys()):  # iterating on a copy since we modify the dict
            matches = re.findall(pattern, str(row[key]))
            mentioned_columns.extend(matches)
            row[key] = substitute_string(row[key], df, locale)

        # Create different types of charts based on the spec
        if row["chartType"] == "VALUE":
            create_value_chart(row, dashboard, datasets[idx], mentioned_columns)

        elif row["chartType"] == "PIE":
            create_pie_chart(row, dashboard, datasets[idx], locale)

        elif "LINES" in row["chartType"]:
            create_lines_chart(row, dashboard, datasets[idx], locale)

        elif "BARS" in row["chartType"]:
            create_bars_chart(row, dashboard, datasets[idx], locale)

        db.session.commit()


def create_value_chart(row, dashboard, dataset, mentioned_columns):
    """Helper function to create and add a VALUE type chart."""
    params = {
        "datasource": f"{dataset.id}__table",
        "viz_type": "handlebars",
        "query_mode": "raw",
        "groupby": [],
        "metrics": [
            {
                "aggregate": None,
                "column": None,
                "datasourceWarning": False,
                "expressionType": "SQL",
                "hasCustomLabel": False,
                "label": ", ".join(mentioned_columns),
                "sqlExpression": ", ".join(mentioned_columns),
            }
        ],
        "all_columns": mentioned_columns,
        "percent_metrics": [],
        "order_by_cols": [],
        "order_desc": True,
        "row_limit": 10000,
        "server_page_length": 10,
        "adhoc_filters": [],
        "handlebarsTemplate": f'{{{{#each data}}}}\n<h1 class="data-chart">\n {{{{this.OBS_VALUE}}}}\n</h1><span class="data-label">{row["Subtitle"]}</span>{{{{/each}}}}\n',
        "styleTemplate": "\n.data-chart {\n  text-align: center;\n  font-size: 6em;\n}\n.data-label {\n  text-align: center;\n  font-size: 2em;\n  width: 100%;\n  display: block;\n}",
        "extra_form_data": {},
        "dashboards": [dashboard.id],
    }
    chart = Slice(
        dashboards=[dashboard],
        datasource_id=dataset.id,
        params=json.dumps(params),
        is_managed_externally=True,
        slice_name=row["Title"],
        datasource_type="table",
        viz_type="handlebars",
    )
    dashboard.slices.append(chart)
    db.session.add(chart)


def create_pie_chart(row, dashboard, dataset, locale):
    """Helper function to create and add a PIE type chart."""
    params = {
        "viz_type": "pie",
        "groupby": [
            get_locale_column_if_exists(dataset.data, row["legendConcept"], locale)
        ],
        "metric": {
            "aggregate": None,
            "column": None,
            "datasourceWarning": False,
            "expressionType": "SQL",
            "hasCustomLabel": False,
            "label": "OBS_VALUE",
            "sqlExpression": "OBS_VALUE",
        },
        "adhoc_filters": [],
        "row_limit": 10000,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels_threshold": 5,
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "right",
        "label_type": "key_value_percent",
        "number_format": "SMART_NUMBER",
        "date_format": "smart_date",
        "show_labels": True,
        "labels_outside": True,
        "label_line": True,
        "outerRadius": 70,
        "innerRadius": 30,
        "extra_form_data": {},
        "dashboards": [str(dashboard.id)],
    }
    chart = Slice(
        dashboards=[dashboard],
        datasource_id=dataset.id,
        is_managed_externally=True,
        slice_name=row["Title"],
        datasource_type="table",
        params=json.dumps(params),
        query_context="",
        viz_type="pie",
    )

    dashboard.slices.append(chart)
    db.session.add(chart)


def create_lines_chart(row, dashboard, dataset, locale):
    """Helper function to create and add a LINES type chart."""
    params = {
        "datasource": f"{dataset.id}__table",
        "viz_type": "echarts_timeseries_line",
        "x_axis": row["xAxisConcept"],
        "time_grain_sqla": "P1D",
        "x_axis_sort_asc": True,
        "x_axis_sort_series": "name",
        "x_axis_sort_series_ascending": True,
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "OBS_VALUE",
                "column": None,
                "aggregate": None,
                "datasourceWarning": False,
                "hasCustomLabel": False,
                "label": "OBS_VALUE",
            }
        ],
        "groupby": [
            get_locale_column_if_exists(dataset.data, row["legendConcept"], locale)
        ],
        "adhoc_filters": [],
        "order_desc": True,
        "row_limit": 10000,
        "truncate_metric": True,
        "show_empty_columns": True,
        "comparison_type": "values",
        "annotation_layers": [],
        "forecastPeriods": 10,
        "forecastInterval": 0.8,
        "x_axis_title_margin": 15,
        "y_axis_title_margin": 15,
        "y_axis_title_position": "Left",
        "sort_series_type": "sum",
        "color_scheme": "supersetColors",
        "seriesType": "line",
        "show_value": False,
        "only_total": True,
        "opacity": 0.2,
        "markerSize": 6,
        "zoomable": False,
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "x_axis_time_format": "smart_date",
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "y_axis_format": "SMART_NUMBER",
        "y_axis_bounds": [None, None],
        "extra_form_data": {},
        "dashboards": [str(dashboard.id)],
    }
    chart = Slice(
        dashboards=[dashboard],
        datasource_id=dataset.id,
        is_managed_externally=True,
        slice_name=row["Title"],
        datasource_type="table",
        params=json.dumps(params),
        query_context="",
        viz_type="echarts_timeseries_line",
    )
    dashboard.slices.append(chart)
    db.session.add(chart)


def create_bars_chart(row, dashboard, dataset, locale):
    """Helper function to create and add a BARS type chart."""
    params = {
        "datasource": f"{dataset.id}__table",
        "viz_type": "echarts_timeseries_bar",
        "x_axis": get_locale_column_if_exists(
            dataset.data, row["xAxisConcept"], locale
        ),
        "time_grain_sqla": "P1D",
        "x_axis_sort": "OBS_VALUE",
        "x_axis_sort_asc": False,
        "x_axis_sort_series": "name",
        "x_axis_sort_series_ascending": True,
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "OBS_VALUE",
                "column": None,
                "aggregate": None,
                "datasourceWarning": False,
                "hasCustomLabel": False,
                "label": "OBS_VALUE",
            }
        ],
        "groupby": [],
        "adhoc_filters": [],
        "order_desc": True,
        "row_limit": 10000,
        "truncate_metric": True,
        "show_empty_columns": True,
        "comparison_type": "values",
        "annotation_layers": [],
        "forecastPeriods": 10,
        "forecastInterval": 0.8,
        "orientation": "vertical",
        "x_axis_title_margin": 15,
        "y_axis_title_margin": 15,
        "y_axis_title_position": "Left",
        "sort_series_type": "sum",
        "color_scheme": "supersetColors",
        "only_total": True,
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "x_axis_time_format": "smart_date",
        "y_axis_format": "SMART_NUMBER",
        "y_axis_bounds": [None, None],
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "extra_form_data": {},
        "dashboards": [str(dashboard.id)],
    }
    chart = Slice(
        dashboards=[dashboard],
        datasource_id=dataset.id,
        is_managed_externally=True,
        slice_name=row["Title"],
        datasource_type="table",
        params=json.dumps(params),
        query_context="",
        viz_type="echarts_timeseries_bar",
    )
    dashboard.slices.append(chart)
    db.session.add(chart)


def _create_codelist_dataframe(codelist, concept_name):
    """
    Convert a codelist dictionary into a list of dictionaries with codelist data.
    :param codelist: A codelist dictionary
    :param concept_name: The name of the concept associated with the codelist
    :return: DataFrame representation of the codelist
    """
    codelist_list = []
    for id, code in codelist.items.items():
        item = {"id": id}
        if isinstance(
            code.name, dict
        ):  # If code name is a dictionary (different languages)
            for lang_code, strings in code.name.items():
                item[f"{concept_name}-{lang_code}"] = strings["content"]
        else:
            item[f"{concept_name}-en"] = code.name  # Default language is English
        codelist_list.append(item)

    return pd.DataFrame(codelist_list)


def _generate_insight_dict(metadata_payload):
    """
    Generate a dictionary containing insights from the metadata payload.
    :param metadata_payload: Metadata payload dictionary
    :return: A dictionary of insights extracted from the metadata
    """
    result = {}
    codelists = metadata_payload["Codelists"]
    structure = metadata_payload["DataStructures"]

    # Ensure that there's only one structure in the payload
    if len(structure) != 1:
        raise Exception("One structure expected")
    structure = list(structure.values())[0]

    for id, component in structure.dimension_descriptor.components.items():
        codelist = component.representation.codelist
        if codelist:
            codelist = _create_codelist_dataframe(codelist, component.id)
        result[id] = {"name": component.concept_identity.name, "codelist": codelist}
    return result


def generate_final_df_and_concepts_name(data, metadata_payload):
    """
    Generate the final DataFrame and the dictionary of concept names.
    :param data: Original data DataFrame
    :param metadata_payload: Metadata payload dictionary
    :return: Final data DataFrame, and concepts names dictionary
    """
    insight_dict = _generate_insight_dict(metadata_payload)

    concepts_names = {}
    for code, component in insight_dict.items():
        concepts_names[code] = component["name"]
        if component["codelist"] is not None:
            data = data.merge(
                component["codelist"], left_on=code, right_on="id", how="inner"
            )
            data.drop(
                columns=["id_x", "id_y"], inplace=True, errors="ignore"
            )  # Remove unnecessary columns

    return data, concepts_names


def get_dataflow_id(sdmx_url):
    """
    Extract the dataflow ID from an SDMX URL.
    :param sdmx_url: The SDMX URL string
    :return: The dataflow ID
    """
    full_dataflow_id = sdmx_url.split("data/")[1].split("/")[0]
    return (
        full_dataflow_id.split(",")[1] if "," in full_dataflow_id else full_dataflow_id
    )


def get_agency_id(sdmx_url):
    """
    Determine the agency ID based on the domain present in the SDMX URL.
    :param sdmx_url: The SDMX URL string
    :return: The agency ID
    """
    agency_mapping = {
        "stats.bis.org": "BIS",
        "ecb.europa.eu": "ECB",
        "ec.europa.eu": "ESTAT",
        "ilo.org": "ILO",
        "oecd.org": "OECD",
        "unicef.org": "UNICEF",
    }

    for domain, agency in agency_mapping.items():
        if domain in sdmx_url:
            return agency
