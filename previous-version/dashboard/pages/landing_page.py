from datetime import datetime

import httpx
from dash import Input, Output, callback, dcc, html, register_page


register_page(
    __name__,
    path="/",
    title="Current Weather",
)


from config import API_BASE_URL

# ---------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------

def format_number(value, unit, decimals=1):
    if value is None:
        return "N/A"

    return f"{value:,.{decimals}f} {unit}"


def format_timestamp(raw_timestamp):
    if not raw_timestamp:
        return "N/A"

    try:
        timestamp = datetime.fromisoformat(raw_timestamp)
        return timestamp.strftime("%I:%M %p")
    except (TypeError, ValueError):
        return str(raw_timestamp)


# ---------------------------------------------------------
# Styles
# ---------------------------------------------------------

CARD_STYLE = {
    "backgroundColor": "#F8F9FA",
    "borderRadius": "12px",
    "padding": "18px",
    "width": "180px",
    "minHeight": "120px",
    "boxShadow": "0px 2px 6px rgba(0, 0, 0, 0.1)",
    "textAlign": "center",
}

CLICKABLE_CARD_STYLE = {
    **CARD_STYLE,
    "cursor": "pointer",
}

CARD_LINK_STYLE = {
    "textDecoration": "none",
    "color": "inherit",
}


STATIONS = [
    ("Waverly", "wx_waverly"),
    ("East Street", "wx_east_st"),
]


# ---------------------------------------------------------
# Reusable metric card
# ---------------------------------------------------------

def create_metric_card(label, value_id, metric):
    return dcc.Link(
        html.Div(
            [
                html.H4(
                    label,
                    style={
                        "marginTop": "5px",
                        "marginBottom": "14px",
                    },
                ),

                html.H2(
                    "--",
                    id=value_id,
                    style={
                        "marginTop": "0",
                        "marginBottom": "10px",
                    },
                ),

                html.P(
                    "View trends →",
                    style={
                        "fontSize": "13px",
                        "color": "#6B7280",
                        "margin": "0",
                    },
                ),
            ],
            style=CLICKABLE_CARD_STYLE,
        ),
        href=f"/trends/{metric}",
        style=CARD_LINK_STYLE,
    )


# ---------------------------------------------------------
# Page layout
# ---------------------------------------------------------

layout = html.Div(
    [
        # -------------------------------------------------
        # Title
        # -------------------------------------------------

        html.H1(
            "Real-Time Weather Analytics Platform",
            style={
                "textAlign": "center",
                "fontSize": "44px",
                "fontWeight": "bold",
                "marginTop": "25px",
                "marginBottom": "5px",
                "fontFamily": "Arial",
            },
        ),

        html.P(
            "Real-time weather observations and historical trends",
            style={
                "textAlign": "center",
                "fontSize": "18px",
                "color": "#666666",
                "marginBottom": "40px",
                "fontFamily": "Arial",
            },
        ),

        # -------------------------------------------------
        # Station dropdown
        # -------------------------------------------------

        html.Div(
            [
                html.Label(
                    "Station",
                    style={
                        "fontSize": "16px",
                        "fontWeight": "600",
                        "marginBottom": "8px",
                        "display": "block",
                        "fontFamily": "Arial",
                    },
                ),

                dcc.Dropdown(
                    id="station-dropdown",
                    options=[
                        {
                            "label": label,
                            "value": value,
                        }
                        for label, value in STATIONS
                    ],
                    value="wx_waverly",
                    clearable=False,
                    searchable=False,
                ),
            ],
            style={
                "width": "320px",
                "margin": "0 auto 40px auto",
            },
        ),

        # -------------------------------------------------
        # Current conditions
        # -------------------------------------------------

        html.H2(
            "Current Conditions",
            style={
                "textAlign": "center",
                "fontFamily": "Arial",
                "marginBottom": "25px",
            },
        ),

        html.Div(
            [
                create_metric_card(
                    label="Temperature",
                    value_id="temperature-value",
                    metric="temperature",
                ),

                create_metric_card(
                    label="Wind Speed",
                    value_id="wind-speed-value",
                    metric="wind_speed",
                ),

                create_metric_card(
                    label="Wind Direction",
                    value_id="wind-direction-value",
                    metric="wind_direction",
                ),

                create_metric_card(
                    label="Rainfall",
                    value_id="rainfall-value",
                    metric="rainfall",
                ),

                create_metric_card(
                    label="Humidity",
                    value_id="humidity-value",
                    metric="humidity",
                ),

                create_metric_card(
                    label="Pressure",
                    value_id="pressure-value",
                    metric="pressure",
                ),

                create_metric_card(
                    label="Lux",
                    value_id="lux-value",
                    metric="lux",
                ),

                # Last Updated is informational only.
                html.Div(
                    [
                        html.H4(
                            "Last Updated",
                            style={
                                "marginTop": "5px",
                                "marginBottom": "14px",
                            },
                        ),

                        html.H2(
                            "--",
                            id="last-updated-value",
                            style={
                                "marginTop": "0",
                                "marginBottom": "10px",
                            },
                        ),
                    ],
                    style=CARD_STYLE,
                ),
            ],
            style={
                "display": "flex",
                "justifyContent": "center",
                "gap": "25px",
                "marginBottom": "45px",
                "flexWrap": "wrap",
            },
        ),
    ],
    style={
        "width": "95%",
        "maxWidth": "1500px",
        "margin": "auto",
        "fontFamily": "Arial",
    },
)


# ---------------------------------------------------------
# Current conditions callback
# ---------------------------------------------------------

@callback(
    Output("temperature-value", "children"),
    Output("wind-speed-value", "children"),
    Output("wind-direction-value", "children"),
    Output("rainfall-value", "children"),
    Output("humidity-value", "children"),
    Output("pressure-value", "children"),
    Output("lux-value", "children"),
    Output("last-updated-value", "children"),
    Input("station-dropdown", "value"),
)
def update_current_conditions(station):
    try:
        response = httpx.get(
            f"{API_BASE_URL}/latest",
            params={
                "station": station,
            },
            timeout=10.0,
        )

        response.raise_for_status()
        records = response.json()

    except (httpx.HTTPError, ValueError) as exc:
        print(
            f"Unable to retrieve latest weather data: {exc}"
        )

        return (
            "Unavailable",
            "Unavailable",
            "Unavailable",
            "Unavailable",
            "Unavailable",
            "Unavailable",
            "Unavailable",
            "Unavailable",
        )

    if not records:
        return (
            "N/A",
            "N/A",
            "N/A",
            "N/A",
            "N/A",
            "N/A",
            "N/A",
            "N/A",
        )

    # The API returns a one-item list when a station is supplied.
    record = records[0]

    temperature = record.get("temperature")
    wind_speed = record.get("wind_speed")
    wind_direction = record.get("wind_direction")
    rain_inches = record.get("rain_inches")
    humidity = record.get("humidity")
    pressure = record.get("pressure")
    lux = record.get("lux")
    latest_timestamp = record.get("dt")

    return (
        format_number(
            temperature,
            "°F",
            decimals=1,
        ),
        format_number(
            wind_speed,
            "mph",
            decimals=1,
        ),
        format_number(
            wind_direction,
            "°",
            decimals=0,
        ),
        format_number(
            rain_inches,
            "in",
            decimals=2,
        ),
        format_number(
            humidity,
            "%",
            decimals=1,
        ),
        format_number(
            pressure,
            "hPa",
            decimals=1,
        ),
        format_number(
            lux,
            "lx",
            decimals=0,
        ),
        format_timestamp(
            latest_timestamp,
        ),
    )