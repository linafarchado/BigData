import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import logging

from datetime import date, timedelta
import plotly.graph_objects as go
import plotly.express as px

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(__name__,  title="Bourse", suppress_callback_exceptions=True) # , external_stylesheets=external_stylesheets)
server = app.server
logging.basicConfig(level=logging.DEBUG)
app.layout = html.Div([

                html.Label('Multi-Select Company Dropdown'),
                dcc.Dropdown(id='company-dropdown', multi=True),  # Update dropdown ID
                html.Button('Update Companies', id='update-companies', n_clicks=0),

                html.Label('Time Period:'),
                dcc.DatePickerRange(
                    id='date-picker-range',
                    min_date_allowed=date(2020, 1, 1),
                    max_date_allowed=date.today(),
                    initial_visible_month=date.today(),
                    start_date=date(2019, 1, 1),
                    end_date=date.today(),
                ),

                html.Label('Scale Type:'),
                dcc.RadioItems(
                    ['Linear', 'Log'],
                    'Linear',
                    id='crossfilter-xaxis-type',
                    labelStyle={'display': 'inline-block', 'marginTop': '5px'}
                ),

                html.Label('Graph Type:'),
                dcc.RadioItems(
                    ['Line', 'Candlestick'],
                    'Line',
                    id='graph-type',
                    labelStyle={'display': 'inline-block', 'marginTop': '5px'}
                ),

                dcc.Graph(id='stock-prices-graph'),

                dcc.Dropdown(
                    id='company-to-display',
                    value=None
                ), 
                html.Div(id='raw-data-table', style={'width': '100%', 'overflow-x': 'scroll'}),

                dcc.Textarea(
                    id='sql-query',
                    value='''
                        SELECT * FROM pg_catalog.pg_tables
                            WHERE schemaname != 'pg_catalog' AND 
                                  schemaname != 'information_schema';
                    ''',
                    style={'width': '100%', 'height': 100},
                    ),
                html.Button('Execute', id='execute-query', n_clicks=0),
                html.Div(id='query-result')
             ])

def get_stocks(symbol):
    query = f"SELECT date, value FROM stocks WHERE cid = (SELECT mid FROM companies WHERE symbol = '{symbol}')"
    return pd.read_sql_query(query, engine)

def get_daystocks(symbol):
    query = f"SELECT date, open, high, low, close FROM daystocks WHERE cid = (SELECT mid FROM companies WHERE symbol = '{symbol}')"
    return pd.read_sql_query(query, engine)

def update_shown_dates(stocks_df, start_date, end_date):
    if start_date and end_date:
        stocks_df = stocks_df[stocks_df['date'] >= start_date]
        stocks_df = stocks_df[stocks_df['date'] <= end_date]
    return stocks_df

def display_raw_data(symbol, company_to_display, stocks_df, table_data):
    if company_to_display == symbol:
        daily_stats = stocks_df.resample('D', on='date').agg({
            'value': ['min', 'max', 'mean', 'std'],
            'date': ['first', 'last']
        }).reset_index()

        daily_stats.columns = [symbol, 'Min', 'Max', 'Mean', 'Std', 'First', 'Last']

        daily_stats['Mean'] = daily_stats['Mean'].apply(lambda x: round(x, 5))
        daily_stats['Std'] = daily_stats['Std'].apply(lambda x: round(x, 5))

        table_data.append(html.Table(
        children=[
            html.Tr([html.Th(col) for col in daily_stats.columns])
        ] + [
            html.Tr([html.Td(val) for val in row]) for _, row in daily_stats.iterrows()
        ]
        ))

    return table_data

def get_candlestick_figure(daystocks_df):
     return go.Figure(go.Candlestick(
        x=daystocks_df['date'],
        open=daystocks_df['open'],
        high=daystocks_df['high'],
        low=daystocks_df['low'],
        close=daystocks_df['close']
    ))


@app.callback(
    ddep.Output('company-dropdown', 'options'),
    ddep.Input('update-companies', 'n_clicks')
)
def update_dropdown_options(n_clicks):
    if n_clicks > 0:
        try:
            company_df = pd.read_sql_query('SELECT name, symbol FROM companies', engine)
            return [{'label': row['name'] + " - " + row['symbol'], 'value': row['symbol']} for _, row in company_df.iterrows()]
        
        except Exception as e:
            return []
    return []

@app.callback(
    [
        ddep.Output('stock-prices-graph', 'figure'),
        ddep.Output('raw-data-table', 'children'),
        ddep.Output('company-to-display', 'options')
    ],
    [
        ddep.Input('company-dropdown', 'value'),
        ddep.Input('crossfilter-xaxis-type', 'value'),
        ddep.Input('date-picker-range', 'start_date'),
        ddep.Input('date-picker-range', 'end_date'),
        ddep.Input('graph-type', 'value'),
        ddep.Input('company-to-display', 'value')
    ]
)
def update_stock_prices_graph(selected_companies, yaxis_type, start_date, end_date, graph_type, company_to_display):
    if selected_companies:
        try:
            stock_data, table_data = [], []
            dropdown_options = [{'label': company, 'value': company} for company in selected_companies]

            for symbol in selected_companies:
                stocks_df = get_stocks(symbol)
                daystocks_df = get_daystocks(symbol)

                if not stocks_df.empty:
                    stocks_df = stocks_df.sort_values(by='date')
                    daystocks_df = daystocks_df.sort_values(by='date')

                    stocks_df = update_shown_dates(stocks_df, start_date, end_date)

                    table_data = display_raw_data(symbol, company_to_display, stocks_df, table_data)

                    if graph_type == 'Candlestick':
                        fig = get_candlestick_figure(daystocks_df)
                        return fig, table_data, dropdown_options
                        #stock_data.append(fig)
                    else:
                        stock_data.append({
                            'x': stocks_df['date'],
                            'y': stocks_df['value'],
                            'name': symbol
                        })

            figure = {
                'data': stock_data,
                'layout': {
                    'yaxis': {'type': 'linear' if yaxis_type == 'Linear' else 'log'}  # Set y-axis type in layout
                }
            }

            return figure, table_data, dropdown_options
        except Exception as e:
            return {}, [], {}
    return {}, [], {}

@app.callback( ddep.Output('query-result', 'children'),
               ddep.Input('execute-query', 'n_clicks'),
               ddep.State('sql-query', 'value'),
             )

def run_query(n_clicks, query):
    if n_clicks > 0:
        try:
            result_df = pd.read_sql_query(query, engine)
            return html.Pre(result_df.to_string())
        
        except Exception as e:
            return html.Pre(str(e))
    return "Enter a query and press execute."

if __name__ == '__main__':
    app.run(debug=True)