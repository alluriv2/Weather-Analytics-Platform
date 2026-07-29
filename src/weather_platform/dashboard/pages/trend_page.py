import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import (
    Input,
    Output,
    State,
    callback,
    ctx,
    dcc,
    html,
    register_page,
)

from weather_platform.config import API_BASE_URL


# ---------------------------------------------------------
# Page registration
# ---------------------------------------------------------

register_page(
    __name__,
    path_template="/trends/<metric>",
    title="Weather Trends",
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------



METRIC_CONFIG = {
    "temperature": {
        "label": "Temperature",
        "unit": "°F",
        "decimals": 1,
    },
    "wind_speed": {
        "label": "Wind Speed",
        "unit": "mph",
        "decimals": 1,
    },
    "rainfall": {
        "label": "Rainfall",
        "unit": "in",
        "decimals": 3,
    },
    "humidity": {
        "label": "Humidity",
        "unit": "%",
        "decimals": 1,
    },
    "pressure": {
        "label": "Pressure",
        "unit": "hPa",
        "decimals": 1,
    },
    "lux": {
        "label": "Lux",
        "unit": "lx",
        "decimals": 0,
    },
}


WINDOW_CONFIG = {
    "day": {
        "label": "24 Hours",
    },
    "week": {
        "label": "1 Week",
    },
    "month": {
        "label": "1 Month",
    },
    "year": {
        "label": "1 Year",
    },
}


STATIONS = [
    ("All Stations", "all"),
    ("Waverly", "wx_waverly"),
    ("East Street", "wx_east_st"),
]


STATION_LABELS = {
    "all": "All Stations",
    "wx_waverly": "Waverly",
    "wx_east_st": "East Street",
}


# ---------------------------------------------------------
# Styles
# ---------------------------------------------------------

BASE_BUTTON_STYLE = {
    "padding": "10px 22px",
    "fontSize": "15px",
    "fontWeight": "600",
    "borderRadius": "8px",
    "border": "1px solid #D1D5DB",
    "backgroundColor": "#F8F9FA",
    "color": "#222222",
    "cursor": "pointer",
    "fontFamily": "Arial",
}


ACTIVE_BUTTON_STYLE = {
    **BASE_BUTTON_STYLE,
    "backgroundColor": "#111827",
    "border": "1px solid #111827",
    "color": "#FFFFFF",
}


PAGE_STYLE = {
    "width": "95%",
    "maxWidth": "1400px",
    "margin": "0 auto",
    "fontFamily": "Arial",
    "paddingBottom": "50px",
}


GRAPH_CONTAINER_STYLE = {
    "backgroundColor": "#FFFFFF",
    "borderRadius": "12px",
    "boxShadow": "0 2px 8px rgba(0, 0, 0, 0.08)",
    "padding": "15px",
    "marginTop": "25px",
}


# ---------------------------------------------------------
# Empty/error chart helper
# ---------------------------------------------------------

def create_empty_figure(message):
    figure = go.Figure()

    figure.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font={
            "size": 17,
            "color": "#6B7280",
        },
    )

    figure.update_layout(
        xaxis={
            "visible": False,
        },
        yaxis={
            "visible": False,
        },
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        margin={
            "l": 30,
            "r": 30,
            "t": 40,
            "b": 30,
        },
    )

    return figure


# ---------------------------------------------------------
# API data retrieval
# ---------------------------------------------------------

def fetch_history_data(
    station: str,
    metric: str,
    window: str,
) -> pd.DataFrame:
    parameters = {
        "window": window,
        "metric": metric,
    }

    # Omitting station causes FastAPI to return all stations.
    if station != "all":
        parameters["station"] = station

    response = httpx.get(
        f"{API_BASE_URL}/history",
        params=parameters,
        timeout=20.0,
    )

    response.raise_for_status()

    records = response.json()

    if not records:
        return pd.DataFrame()

    dataframe = pd.DataFrame(records)

    required_columns = {
        "bucket_ts",
        "station",
        metric,
    }

    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise ValueError(
            "API response is missing columns: "
            + ", ".join(sorted(missing_columns))
        )

    dataframe["bucket_ts"] = pd.to_datetime(
        dataframe["bucket_ts"],
        errors="coerce",
    )

    dataframe[metric] = pd.to_numeric(
        dataframe[metric],
        errors="coerce",
    )

    dataframe = (
        dataframe
        .dropna(
            subset=[
                "bucket_ts",
                metric,
            ]
        )
        .sort_values(
            [
                "station",
                "bucket_ts",
            ]
        )
    )

    dataframe["station_label"] = dataframe["station"].map(
        STATION_LABELS
    ).fillna(dataframe["station"])

    # The aggregate table stores pressure in Pa.
    # Convert it to hPa for display.
    if metric == "pressure":
        dataframe[metric] = dataframe[metric] / 100

    return dataframe


# ---------------------------------------------------------
# Page layout
# ---------------------------------------------------------

def layout(metric=None, **kwargs):
    if metric not in METRIC_CONFIG:
        return html.Div(
            [
                html.H2(
                    "Metric not found",
                    style={
                        "textAlign": "center",
                        "marginTop": "80px",
                    },
                ),

                html.P(
                    "The requested weather metric is not supported.",
                    style={
                        "textAlign": "center",
                        "color": "#6B7280",
                    },
                ),

                html.Div(
                    dcc.Link(
                        "← Return to Current Conditions",
                        href="/",
                        style={
                            "textDecoration": "none",
                            "fontWeight": "600",
                            "color": "#374151",
                        },
                    ),
                    style={
                        "textAlign": "center",
                        "marginTop": "25px",
                    },
                ),
            ],
            style=PAGE_STYLE,
        )

    metric_config = METRIC_CONFIG[metric]

    return html.Div(
        [
            dcc.Store(
                id="trend-selected-metric",
                data=metric,
            ),

            dcc.Store(
                id="trend-selected-window",
                data="day",
            ),

            # -------------------------------------------------
            # Navigation
            # -------------------------------------------------

            html.Div(
                [
                    dcc.Link(
                        "← Current Conditions",
                        href="/",
                        style={
                            "textDecoration": "none",
                            "fontWeight": "600",
                            "color": "#374151",
                        },
                    ),
                ],
                style={
                    "marginTop": "25px",
                    "marginBottom": "15px",
                },
            ),

            # -------------------------------------------------
            # Heading
            # -------------------------------------------------

            html.H1(
                f"{metric_config['label']} Trends",
                style={
                    "textAlign": "center",
                    "fontSize": "42px",
                    "marginBottom": "8px",
                },
            ),

            html.P(
                (
                    f"Explore historical "
                    f"{metric_config['label'].lower()} observations "
                    "across multiple time windows."
                ),
                style={
                    "textAlign": "center",
                    "fontSize": "17px",
                    "color": "#6B7280",
                    "marginBottom": "35px",
                },
            ),

            # -------------------------------------------------
            # Station selector
            # -------------------------------------------------

            html.Div(
                [
                    html.Label(
                        "Station",
                        style={
                            "fontSize": "16px",
                            "fontWeight": "600",
                            "display": "block",
                            "marginBottom": "8px",
                        },
                    ),

                    dcc.Dropdown(
                        id="trend-station-dropdown",
                        options=[
                            {
                                "label": label,
                                "value": value,
                            }
                            for label, value in STATIONS
                        ],
                        value="all",
                        clearable=False,
                        searchable=False,
                    ),
                ],
                style={
                    "width": "320px",
                    "margin": "0 auto",
                },
            ),

            # -------------------------------------------------
            # Chart
            # -------------------------------------------------

            html.Div(
                [
                    dcc.Loading(
                        type="circle",
                        children=dcc.Graph(
                            id="weather-trend-chart",
                            config={
                                "displaylogo": False,
                                "scrollZoom": True,
                                "modeBarButtonsToRemove": [
                                    "lasso2d",
                                    "select2d",
                                ],
                            },
                            style={
                                "height": "520px",
                            },
                        ),
                    ),
                ],
                style=GRAPH_CONTAINER_STYLE,
            ),

            # -------------------------------------------------
            # Window buttons
            # -------------------------------------------------

            html.Div(
                [
                    html.Button(
                        "24 Hours",
                        id="trend-window-day",
                        n_clicks=0,
                        style=ACTIVE_BUTTON_STYLE,
                    ),

                    html.Button(
                        "1 Week",
                        id="trend-window-week",
                        n_clicks=0,
                        style=BASE_BUTTON_STYLE,
                    ),

                    html.Button(
                        "1 Month",
                        id="trend-window-month",
                        n_clicks=0,
                        style=BASE_BUTTON_STYLE,
                    ),

                    html.Button(
                        "1 Year",
                        id="trend-window-year",
                        n_clicks=0,
                        style=BASE_BUTTON_STYLE,
                    ),
                ],
                style={
                    "display": "flex",
                    "justifyContent": "center",
                    "gap": "15px",
                    "marginTop": "25px",
                    "flexWrap": "wrap",
                },
            ),
        ],
        style=PAGE_STYLE,
    )


# ---------------------------------------------------------
# Window selection callback
# ---------------------------------------------------------

@callback(
    Output("trend-selected-window", "data"),
    Output("trend-window-day", "style"),
    Output("trend-window-week", "style"),
    Output("trend-window-month", "style"),
    Output("trend-window-year", "style"),
    Input("trend-window-day", "n_clicks"),
    Input("trend-window-week", "n_clicks"),
    Input("trend-window-month", "n_clicks"),
    Input("trend-window-year", "n_clicks"),
    State("trend-selected-window", "data"),
)
def select_window(
    day_clicks,
    week_clicks,
    month_clicks,
    year_clicks,
    current_window,
):
    triggered_button = ctx.triggered_id

    button_to_window = {
        "trend-window-day": "day",
        "trend-window-week": "week",
        "trend-window-month": "month",
        "trend-window-year": "year",
    }

    selected_window = button_to_window.get(
        triggered_button,
        current_window or "day",
    )

    styles = {
        window: (
            ACTIVE_BUTTON_STYLE
            if selected_window == window
            else BASE_BUTTON_STYLE
        )
        for window in WINDOW_CONFIG
    }

    return (
        selected_window,
        styles["day"],
        styles["week"],
        styles["month"],
        styles["year"],
    )


# ---------------------------------------------------------
# Chart callback
# ---------------------------------------------------------

@callback(
    Output("weather-trend-chart", "figure"),
    Input("trend-station-dropdown", "value"),
    Input("trend-selected-metric", "data"),
    Input("trend-selected-window", "data"),
)
def update_trend_chart(
    station,
    metric,
    window,
):
    if metric not in METRIC_CONFIG:
        return create_empty_figure(
            "Unsupported weather metric."
        )

    if window not in WINDOW_CONFIG:
        return create_empty_figure(
            "Unsupported time window."
        )

    metric_config = METRIC_CONFIG[metric]
    window_config = WINDOW_CONFIG[window]

    try:
        dataframe = fetch_history_data(
            station=station,
            metric=metric,
            window=window,
        )

    except httpx.HTTPStatusError as exc:
        detail = ""

        try:
            detail = exc.response.json().get(
                "detail",
                "",
            )
        except ValueError:
            pass

        print(
            "Historical API request failed: "
            f"{exc}. {detail}"
        )

        return create_empty_figure(
            "Unable to retrieve historical weather data."
        )

    except (httpx.HTTPError, ValueError) as exc:
        print(
            f"Unable to retrieve historical weather data: {exc}"
        )

        return create_empty_figure(
            "Unable to retrieve historical weather data."
        )

    if dataframe.empty:
        return create_empty_figure(
            "No historical data is available for this selection."
        )

    station_label = STATION_LABELS.get(
        station,
        station,
    )

    chart_arguments = {
        "data_frame": dataframe,
        "x": "bucket_ts",
        "y": metric,
        "title": (
            f"{metric_config['label']} — "
            f"{station_label} — "
            f"{window_config['label']}"
        ),
        "labels": {
            "bucket_ts": "Time",
            metric: (
                f"{metric_config['label']} "
                f"({metric_config['unit']})"
            ),
            "station_label": "Station",
        },
    }

    # Show one line per station when All Stations is selected.
    if station == "all":
        chart_arguments["color"] = "station_label"

    figure = px.line(
        **chart_arguments,
    )

    decimals = metric_config["decimals"]

    figure.update_traces(
        line={
            "width": 2.5,
        },
        hovertemplate=(
            "<b>%{x}</b><br>"
            + metric_config["label"]
            + f": %{{y:,.{decimals}f}} "
            + metric_config["unit"]
            + "<extra></extra>"
        ),
    )

    figure.update_layout(
        hovermode="x unified",
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        margin={
            "l": 65,
            "r": 30,
            "t": 70,
            "b": 55,
        },
        title={
            "x": 0.5,
            "xanchor": "center",
        },
        font={
            "family": "Arial",
        },
        legend={
            "title": {
                "text": "Station",
            },
        },
    )

    figure.update_xaxes(
        rangeslider_visible=False,
        showgrid=True,
        gridcolor="#E5E7EB",
    )

    figure.update_yaxes(
        showgrid=True,
        gridcolor="#E5E7EB",
        rangemode=(
            "tozero"
            if metric in {
                "wind_speed",
                "rainfall",
                "humidity",
                "lux",
            }
            else "normal"
        ),
    )

    return figure
