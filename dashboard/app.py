from dash import Dash, html, page_container

from config import DASH_HOST, DASH_PORT


app = Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
)

server = app.server

app.title = "Real-Time Weather Analytics Platform"

app.layout = html.Div(
    [
        page_container,
    ]
)


if __name__ == "__main__":
    app.run(
        host=DASH_HOST,
        port=DASH_PORT,
        debug=True,
    )