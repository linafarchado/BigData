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

frequency_options = {
    '10 Minutes': '10min',
    'Hourly': 'h',
    'Daily': 'D',
    'Weekly': 'W',
    'Monthly': 'ME',
    'Yearly': 'YE'
}

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

                html.Label('Show Bollinger Bands:'),
                dcc.Checklist(['Bollinger Bands'], [],
                    id='show-bollinger-bands'
                ),
                html.Label('Bollinger Bands Window:'),
                dcc.Input(
                    id='bollinger-window',
                    type='number',
                    value=20
                ),
                html.Label('Data Frenquency:'),
                dcc.Dropdown(
                id='resample-frequency',
                options=[{'label': k, 'value': v} for k, v in frequency_options.items()],
                value='10min'
            ),
            html.Div(id='output-container', children=[]),

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

def update_frequence_data(stocks_df, frequency):
    daily_stats = stocks_df.resample(frequency, on='date').agg({
            'value': ['last']
        }).reset_index()
    daily_stats.columns = ['date', 'value']

    return daily_stats.dropna()

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

def calculate_bollinger_bands(stocks_df, window=20):
    stocks_df['mean'] = stocks_df['close'].rolling(window).mean()
    stocks_df['std'] = stocks_df['close'].rolling(window).std()
    upper_band = stocks_df['mean'] + (2 * stocks_df['std'])
    lower_band = stocks_df['mean'] - (2 * stocks_df['std'])

    upper_band, lower_band = upper_band.to_frame(name='Upper Bollinger Band'), lower_band.to_frame(name='Lower Bollinger Band')

    upper_band_trace = {
        'x': stocks_df['date'],
        'y': upper_band['Upper Bollinger Band'],
        'name': 'Upper Band',
        'fill' : 'topreviousy',
        'fillcolor': 'rgba(217, 217, 217, 0.5)',
        'showlegend': False,
        'line': {'color': 'rgba(0, 0, 102, 0.3)'}
    }
    lower_band_trace = {
        'x': stocks_df['date'],
        'y': lower_band['Lower Bollinger Band'],
        'fill' : 'tonexty',
        'fillcolor': 'rgba(217, 217, 217, 0.5)',
        'name': 'Lower Band',
        'showlegend': False,
        'line': {'color': 'rgba(128, 0, 0, 0.3)'}
    }
    average_trace = {
        'x': stocks_df['date'],
        'y': stocks_df['mean'],
        'name': 'Simple Moving Average',
        'showlegend': False,
        'line': {'color': 'rgba(0, 0, 0, 0.3)'}
    }
    
    return upper_band_trace, lower_band_trace, average_trace

def create_line_data(frequency_df, graph_type, frequency, symbol):
    daily_stats = frequency_df.resample(frequency, on='date').agg({
            'value': ['first', 'max', 'min', 'last']
        }).reset_index()
    daily_stats.columns = ['date', 'open', 'high', 'low', 'close']
    daily_stats.dropna(inplace=True)
    
    if graph_type == 'Candlestick':
        line_data = {
            'x': daily_stats['date'],
            'open': daily_stats['open'],
            'high': daily_stats['high'],
            'low': daily_stats['low'],
            'close': daily_stats['close'],
            'type': 'candlestick',
            'name': symbol
        }
    else:
        line_data = {
            'x': daily_stats['date'],
            'y': daily_stats['close'],
            'name': symbol
        }

    return line_data, daily_stats

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
        ddep.Input('company-to-display', 'value'),
        ddep.Input('show-bollinger-bands', 'value'),
        ddep.Input('bollinger-window', 'value'),
        ddep.Input('resample-frequency', 'value')
    ]
)
def update_stock_prices_graph(selected_companies, yaxis_type, start_date, end_date, graph_type, company_to_display, show_bollinger_bands, bollinger_window, frequency):
    if selected_companies:
        # try:
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

                    line_data, frequency_df = create_line_data(stocks_df.copy(), graph_type, frequency, symbol)

                    if 'Bollinger Bands' in show_bollinger_bands:
                        upper_band, lower_band, sma_line = calculate_bollinger_bands(frequency_df.copy(), bollinger_window)

                        stock_data.extend([line_data, upper_band, lower_band, sma_line])
                    else:
                        stock_data.append(line_data)

            figure = {
                'data': stock_data,
                'layout': {
                    'yaxis': {'type': 'linear' if yaxis_type == 'Linear' else 'log'},
                    'xaxis': {'rangeslider': {'visible': False}}
                }
            }

            return figure, table_data, dropdown_options
        # except Exception as e:
        #     return {}, [], {}
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