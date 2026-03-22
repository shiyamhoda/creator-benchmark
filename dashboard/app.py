import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, dash_table, Input, Output, callback
from dash.dash_table.Format import Format, Scheme

# ── Path setup ────────────────────────────────────────────────────────────────
# Allows dashboard/app.py to import from src/
sys.path.append(str(Path(__file__).parent.parent))

from src.analytics import (
    get_engine,
    get_niche_benchmarks,
    get_channel_comparison,
    get_top_videos_per_niche,
    get_engagement_vs_subscribers,
    get_niche_engagement_distribution,
    get_nlp_audit_trail,
)

# ── Load data once at startup ────────────────────────────────────────────────
engine       = get_engine()
df_benchmarks   = get_niche_benchmarks(engine)
df_channels     = get_channel_comparison(engine)
df_top_videos   = get_top_videos_per_niche(engine, limit=5)
df_engagement   = get_engagement_vs_subscribers(engine)
df_distribution = get_niche_engagement_distribution(engine)
df_nlp          = get_nlp_audit_trail(engine)

NICHES = sorted(df_channels["niche"].unique().tolist())

# ── Colour palette — one colour per niche ────────────────────────────────────
NICHE_COLORS = {
    "Finance":   "#534AB7",
    "Fitness":   "#1D9E75",
    "Beauty":    "#D4537E",
    "Gaming":    "#D85A30",
    "Tech":      "#185FA5",
    "Food":      "#BA7517",
    "Travel":    "#639922",
    "Education": "#888780",
}

# ── App initialisation ────────────────────────────────────────────────────────
app = Dash(
    __name__,
    title="Creator Economy Benchmarker",
    suppress_callback_exceptions=True,
)
# ── Shared styles ─────────────────────────────────────────────────────────────
CARD = {
    "background": "#ffffff",
    "border":     "1px solid #e8e6e0",
    "borderRadius": "12px",
    "padding":    "20px",
    "marginBottom": "16px",
}

HEADER = {
    "background":   "#ffffff",
    "borderBottom": "1px solid #e8e6e0",
    "padding":      "16px 32px",
    "display":      "flex",
    "alignItems":   "center",
    "justifyContent": "space-between",
    "marginBottom": "0",
}

PAGE = {
    "background":  "#f5f4ef",
    "minHeight":   "100vh",
    "fontFamily":  "system-ui, -apple-system, sans-serif",
    "color":       "#2c2c2a",
}

KPI_CARD = {
    **CARD,
    "textAlign":     "center",
    "padding":       "16px",
    "marginBottom":  "0",
}

TABLE_STYLE = {
    "fontFamily":  "system-ui, -apple-system, sans-serif",
    "fontSize":    "13px",
}

# ── KPI summary bar ───────────────────────────────────────────────────────────
def kpi_bar():
    nlp_accuracy = round(df_nlp["niche_match"].mean() * 100, 1)
    kpis = [
        ("73",          "Channels analysed"),
        ("700",         "Videos analysed"),
        ("8",           "Niches covered"),
        (f"{nlp_accuracy}%", "NLP accuracy"),
    ]
    return html.Div(
        style={"display": "grid", "gridTemplateColumns": "repeat(4,1fr)",
               "gap": "12px", "padding": "20px 32px 0"},
        children=[
            html.Div(style=KPI_CARD, children=[
                html.Div(val, style={"fontSize": "28px", "fontWeight": "500",
                                     "color": "#534AB7"}),
                html.Div(label, style={"fontSize": "12px", "color": "#888780",
                                       "marginTop": "4px"}),
            ]) for val, label in kpis
        ]
    )


# ── Page 1 — Niche Overview ───────────────────────────────────────────────────
def page_niche_overview():
    fig_subs = px.bar(
        df_benchmarks.sort_values("avg_subs_millions"),
        x="avg_subs_millions", y="niche", orientation="h",
        color="niche", color_discrete_map=NICHE_COLORS,
        labels={"avg_subs_millions": "Avg subscribers (millions)", "niche": ""},
        title="Average subscribers by niche",
    ).update_layout(showlegend=False, plot_bgcolor="white",
                    paper_bgcolor="white", height=320)

    fig_eng = px.bar(
        df_benchmarks.sort_values("avg_engagement_rate"),
        x="avg_engagement_rate", y="niche", orientation="h",
        color="niche", color_discrete_map=NICHE_COLORS,
        labels={"avg_engagement_rate": "Avg engagement rate (%)", "niche": ""},
        title="Average engagement rate by niche",
    ).update_layout(showlegend=False, plot_bgcolor="white",
                    paper_bgcolor="white", height=320)

    fig_uploads = px.bar(
        df_benchmarks.sort_values("avg_videos_per_month"),
        x="avg_videos_per_month", y="niche", orientation="h",
        color="niche", color_discrete_map=NICHE_COLORS,
        labels={"avg_videos_per_month": "Avg uploads per month", "niche": ""},
        title="Upload frequency by niche",
    ).update_layout(showlegend=False, plot_bgcolor="white",
                    paper_bgcolor="white", height=320)

    # Normalised heatmap
    metrics    = ["avg_subs_millions", "avg_engagement_rate", "avg_videos_per_month"]
    heatmap_df = df_benchmarks.set_index("niche")[metrics].copy()
    heatmap_norm = (heatmap_df - heatmap_df.min()) / (heatmap_df.max() - heatmap_df.min())
    fig_heat = px.imshow(
        heatmap_norm.T,
        labels={"color": "Normalised score"},
        color_continuous_scale="Purples",
        title="Normalised benchmark heatmap",
        aspect="auto",
    ).update_layout(plot_bgcolor="white", paper_bgcolor="white", height=260)

    return html.Div(style={"padding": "20px 32px"}, children=[
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                        "gap": "16px"},
                 children=[
                     html.Div(style=CARD, children=[dcc.Graph(figure=fig_subs,  config={"displayModeBar": False})]),
                     html.Div(style=CARD, children=[dcc.Graph(figure=fig_eng,   config={"displayModeBar": False})]),
                     html.Div(style=CARD, children=[dcc.Graph(figure=fig_uploads, config={"displayModeBar": False})]),
                     html.Div(style=CARD, children=[dcc.Graph(figure=fig_heat,  config={"displayModeBar": False})]),
                 ]),
    ])


# ── Page 2 — Channel Deep-Dive ────────────────────────────────────────────────
def page_channel_deepdive():
    return html.Div(style={"padding": "20px 32px"}, children=[
        html.Div(style=CARD, children=[
            html.Label("Filter by niche", style={"fontSize": "12px",
                                                  "color": "#888780",
                                                  "marginBottom": "6px",
                                                  "display": "block"}),
            dcc.Dropdown(
                id="niche-filter",
                options=[{"label": "All niches", "value": "ALL"}] +
                        [{"label": n, "value": n} for n in NICHES],
                value="ALL",
                clearable=False,
                style={"maxWidth": "320px"},
            ),
        ]),
        html.Div(style=CARD, children=[
            dcc.Graph(id="scatter-engagement", config={"displayModeBar": False}),
        ]),
        html.Div(style=CARD, children=[
            html.Div("Channel rankings", style={"fontSize": "13px",
                                                 "fontWeight": "500",
                                                 "marginBottom": "12px"}),
            dash_table.DataTable(
                id="channels-table",
                columns=[
                    {"name": "Channel",        "id": "title"},
                    {"name": "Niche",          "id": "niche"},
                    {"name": "Subscribers",    "id": "subscriber_count",
                     "type": "numeric",
                     "format": Format(group=True)},
                    {"name": "Avg engagement %", "id": "avg_engagement_rate",
                     "type": "numeric",
                     "format": Format(precision=2, scheme=Scheme.fixed)},
                    {"name": "Videos analysed", "id": "videos_analysed"},
                ],
                page_size=15,
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_cell={**TABLE_STYLE, "padding": "8px 12px",
                            "border": "1px solid #e8e6e0"},
                style_header={"fontWeight": "500", "background": "#f5f4ef",
                              "border": "1px solid #e8e6e0"},
                style_data_conditional=[{
                    "if": {"row_index": "odd"},
                    "backgroundColor": "#fafaf8",
                }],
            ),
        ]),
    ])


# ── Page 3 — Top Videos ───────────────────────────────────────────────────────
def page_top_videos():
    return html.Div(style={"padding": "20px 32px"}, children=[
        html.Div(style=CARD, children=[
            html.Label("Select niche", style={"fontSize": "12px",
                                               "color": "#888780",
                                               "marginBottom": "6px",
                                               "display": "block"}),
            dcc.Dropdown(
                id="video-niche-filter",
                options=[{"label": n, "value": n} for n in NICHES],
                value=NICHES[0],
                clearable=False,
                style={"maxWidth": "320px"},
            ),
        ]),
        html.Div(style=CARD, children=[
            dcc.Graph(id="top-videos-chart", config={"displayModeBar": False}),
        ]),
        html.Div(style=CARD, children=[
            dash_table.DataTable(
                id="videos-table",
                columns=[
                    {"name": "Video title",    "id": "title"},
                    {"name": "Channel",        "id": "channel"},
                    {"name": "Views",          "id": "view_count",
                     "type": "numeric", "format": Format(group=True)},
                    {"name": "Likes",          "id": "like_count",
                     "type": "numeric", "format": Format(group=True)},
                    {"name": "Engagement %",   "id": "engagement_rate",
                     "type": "numeric",
                     "format": Format(precision=2, scheme=Scheme.fixed)},
                    {"name": "Published",      "id": "published_at"},
                ],
                page_size=10,
                sort_action="native",
                style_table={"overflowX": "auto"},
                style_cell={**TABLE_STYLE, "padding": "8px 12px",
                            "border": "1px solid #e8e6e0",
                            "maxWidth":  "300px",
                            "overflow":  "hidden",
                            "textOverflow": "ellipsis"},
                style_header={"fontWeight": "500", "background": "#f5f4ef",
                              "border": "1px solid #e8e6e0"},
                style_data_conditional=[{
                    "if": {"row_index": "odd"},
                    "backgroundColor": "#fafaf8",
                }],
            ),
        ]),
    ])


# ── Page 4 — NLP Audit Trail ──────────────────────────────────────────────────
def page_nlp_audit():
    accuracy   = round(df_nlp["niche_match"].mean() * 100, 1)
    match_counts = df_nlp.groupby(["api_niche", "niche_match"]).size().reset_index(name="count")
    match_counts["result"] = match_counts["niche_match"].map({True: "Matched", False: "Mismatch"})

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=accuracy,
        title={"text": "Overall NLP accuracy"},
        gauge={
            "axis":  {"range": [0, 100]},
            "bar":   {"color": "#534AB7"},
            "steps": [
                {"range": [0,  60], "color": "#FAECE7"},
                {"range": [60, 80], "color": "#FAEEDA"},
                {"range": [80, 100],"color": "#EAF3DE"},
            ],
        },
        number={"suffix": "%"},
    )).update_layout(height=280, paper_bgcolor="white")

    fig_match = px.bar(
        match_counts,
        x="api_niche", y="count", color="result",
        color_discrete_map={"Matched": "#1D9E75", "Mismatch": "#D85A30"},
        labels={"api_niche": "", "count": "Channels", "result": ""},
        title="NLP match vs mismatch by niche",
    ).update_layout(plot_bgcolor="white", paper_bgcolor="white",
                    height=300, legend={"orientation": "h", "y": -0.2})

    return html.Div(style={"padding": "20px 32px"}, children=[
        html.Div(style={"display": "grid",
                        "gridTemplateColumns": "320px 1fr", "gap": "16px"},
                 children=[
                     html.Div(style=CARD, children=[
                         dcc.Graph(figure=fig_gauge, config={"displayModeBar": False})
                     ]),
                     html.Div(style=CARD, children=[
                         dcc.Graph(figure=fig_match, config={"displayModeBar": False})
                     ]),
                 ]),
        html.Div(style=CARD, children=[
            html.Div("Classification detail", style={"fontSize": "13px",
                                                      "fontWeight": "500",
                                                      "marginBottom": "12px"}),
            dash_table.DataTable(
                columns=[
                    {"name": "Channel",        "id": "title"},
                    {"name": "API niche",       "id": "api_niche"},
                    {"name": "NLP niche",       "id": "nlp_niche"},
                    {"name": "Confidence %",    "id": "confidence_pct",
                     "type": "numeric",
                     "format": Format(precision=1, scheme=Scheme.fixed)},
                    {"name": "Match",           "id": "niche_match"},
                    {"name": "Description preview", "id": "description_preview"},
                ],
                data=df_nlp.to_dict("records"),
                page_size=15,
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_cell={**TABLE_STYLE, "padding": "8px 12px",
                            "border": "1px solid #e8e6e0",
                            "maxWidth": "250px", "overflow": "hidden",
                            "textOverflow": "ellipsis"},
                style_header={"fontWeight": "500", "background": "#f5f4ef",
                              "border": "1px solid #e8e6e0"},
                style_data_conditional=[
                    {"if": {"filter_query": '{niche_match} = "True"'},
                     "backgroundColor": "#EAF3DE"},
                    {"if": {"filter_query": '{niche_match} = "False"'},
                     "backgroundColor": "#FAECE7"},
                    {"if": {"row_index": "odd"}, "backgroundColor": "#fafaf8"},
                ],
            ),
        ]),
    ])

# ── App layout ────────────────────────────────────────────────────────────────
app.layout = html.Div(style=PAGE, children=[

    # Header
    html.Div(style=HEADER, children=[
        html.Div(children=[
            html.Div("Creator Economy Benchmarker",
                     style={"fontSize": "18px", "fontWeight": "500",
                            "color": "#2c2c2a"}),
            html.Div("YouTube niche intelligence — 73 channels · 700 videos · 8 niches",
                     style={"fontSize": "12px", "color": "#888780", "marginTop": "2px"}),
        ]),
        dcc.Tabs(
            id="tabs",
            value="overview",
            style={"border": "none"},
            children=[
                dcc.Tab(label="Niche Overview",    value="overview",
                        style={"border": "none", "padding": "8px 16px"},
                        selected_style={"border": "none", "padding": "8px 16px",
                                        "borderBottom": "2px solid #534AB7",
                                        "fontWeight": "500", "color": "#534AB7"}),
                dcc.Tab(label="Channel Deep-Dive", value="channels",
                        style={"border": "none", "padding": "8px 16px"},
                        selected_style={"border": "none", "padding": "8px 16px",
                                        "borderBottom": "2px solid #534AB7",
                                        "fontWeight": "500", "color": "#534AB7"}),
                dcc.Tab(label="Top Videos",        value="videos",
                        style={"border": "none", "padding": "8px 16px"},
                        selected_style={"border": "none", "padding": "8px 16px",
                                        "borderBottom": "2px solid #534AB7",
                                        "fontWeight": "500", "color": "#534AB7"}),
                dcc.Tab(label="NLP Audit",         value="nlp",
                        style={"border": "none", "padding": "8px 16px"},
                        selected_style={"border": "none", "padding": "8px 16px",
                                        "borderBottom": "2px solid #534AB7",
                                        "fontWeight": "500", "color": "#534AB7"}),
            ],
        ),
    ]),

    # KPI bar — always visible
    kpi_bar(),

    # Page content — swapped by tab callback
    html.Div(id="page-content"),
])
# ── Callbacks ─────────────────────────────────────────────────────────────────

@app.callback(
    Output("page-content", "children"),
    Input("tabs", "value"),
)
def render_page(tab):
    """Swap page content when a tab is clicked."""
    if tab == "overview":  return page_niche_overview()
    if tab == "channels":  return page_channel_deepdive()
    if tab == "videos":    return page_top_videos()
    if tab == "nlp":       return page_nlp_audit()
    return page_niche_overview()


@app.callback(
    Output("scatter-engagement", "figure"),
    Output("channels-table",     "data"),
    Input("niche-filter",        "value"),
)
def update_channel_deepdive(niche):
    """Filter scatter plot and table by selected niche."""
    filtered = (
        df_channels if niche == "ALL"
        else df_channels[df_channels["niche"] == niche]
    )

    fig = px.scatter(
        filtered,
        x="subscriber_count",
        y="avg_engagement_rate",
        color="niche",
        color_discrete_map=NICHE_COLORS,
        hover_name="title",
        hover_data={"subscriber_count": True, "avg_engagement_rate": True,
                    "niche": True},
        labels={"subscriber_count":    "Subscribers",
                "avg_engagement_rate": "Avg engagement rate (%)"},
        title="Subscribers vs engagement rate",
        size_max=18,
    ).update_traces(
        marker={"size": 10, "opacity": 0.8}
    ).update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=420,
        xaxis={"tickformat": ",.0f"},
    )

    return fig, filtered.to_dict("records")


@app.callback(
    Output("top-videos-chart", "figure"),
    Output("videos-table",     "data"),
    Input("video-niche-filter","value"),
)
def update_top_videos(niche):
    """Filter top videos chart and table by selected niche."""
    filtered = df_top_videos[df_top_videos["niche"] == niche].head(5)

    fig = px.bar(
        filtered.sort_values("view_count"),
        x="view_count",
        y="title",
        orientation="h",
        color_discrete_sequence=[NICHE_COLORS.get(niche, "#534AB7")],
        labels={"view_count": "Views", "title": ""},
        title=f"Top 5 videos — {niche}",
        hover_data={"channel": True, "engagement_rate": True},
    ).update_layout(
        showlegend=False,
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=360,
        xaxis={"tickformat": ",.0f"},
        yaxis={"tickmode": "linear",
               "tickfont": {"size": 11}},
    ).update_traces(
        texttemplate="%{x:,.0f}",
        textposition="outside",
    )

    return fig, filtered.to_dict("records")

# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=8050)


