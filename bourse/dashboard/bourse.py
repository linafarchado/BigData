import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy

from datetime import date, timedelta
import plotly.graph_objects as go
import plotly.express as px

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(__name__,  title="Bourse", suppress_callback_exceptions=True) # , external_stylesheets=external_stylesheets)
server = app.server
app.layout = html.Div([

                html.Label('Multi-Select Company Dropdown'),
                dcc.Dropdown(id='company-dropdown', multi=True),  # Update dropdown ID
                html.Button('Update Companies', id='update-companies', n_clicks=0),

                html.Label('Time Period:'),
                dcc.DatePickerRange(
                    id='date-picker-range',
                    min_date_allowed=date(2020, 1, 1),  # Set a minimum allowed date (optional)
                    max_date_allowed=date.today(),  # Set a maximum allowed date (optional)
                    initial_visible_month=date.today(),  # Set the initially visible month
                    start_date=date(2020, 1, 1),  # Set initial start date (last 30 days)
                    end_date=date.today(),  # Set initial end date (today)
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

                dcc.Graph(id='stock-prices-graph'),  # Add a graph component

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

@app.callback(
    ddep.Output('company-dropdown', 'options'),  # Update output component
    ddep.Input('update-companies', 'n_clicks')  # Update trigger
)
def update_dropdown_options(n_clicks):
    if n_clicks > 0:
        try:
            # Assuming your initial query retrieves company names
            company_df = pd.read_sql_query('SELECT name, symbol FROM companies', engine)
            return [{'label': row['name'] + " - " + row['symbol'], 'value': row['symbol']} for _, row in company_df.iterrows()]
        except Exception as e:
            return []  # Return an empty list in case of errors
    return []  # Return an empty list initially

@app.callback(
    ddep.Output('stock-prices-graph', 'figure'),  # Update graph figure
    ddep.Input('company-dropdown', 'value'),  # Trigger on dropdown change and radio button change
    ddep.Input('crossfilter-xaxis-type', 'value'),  # Additional trigger for radio buttons
    ddep.Input('date-picker-range', 'start_date'),  # New input for start date
    ddep.Input('date-picker-range', 'end_date'), # New input for end date
    ddep.Input('graph-type', 'value') # New input for graph type selection
)
def update_stock_prices_graph(selected_companies, yaxis_type, start_date, end_date, graph_type):
    if selected_companies:
        try:
            # Assuming your "stocks" table has "date" and "value" columns
            stock_data = []

            for company in selected_companies:
                query = f"SELECT date, value FROM stocks WHERE cid = (SELECT mid FROM companies WHERE symbol = '{company}')"
                company_df = pd.read_sql_query(query, engine)

                query = f"SELECT date, open, high, low, close FROM daystocks WHERE cid = (SELECT mid FROM companies WHERE symbol = '{company}')"
                daystocks_df = pd.read_sql_query(query, engine)

                if not company_df.empty:  # Check if data exists for the company
                    company_df = company_df.sort_values(by='date')  # Sort by date

                    if start_date and end_date:
                        company_df = company_df[company_df['date'] >= start_date]
                        company_df = company_df[company_df['date'] <= end_date]

                    if graph_type == 'Candlestick':
                        fig = go.Figure(go.Candlestick(
                            x=daystocks_df['date'],
                            open=daystocks_df['open'],
                            high=daystocks_df['high'],
                            low=daystocks_df['low'],
                            close=daystocks_df['close']
                        ))

                        return fig
                        #stock_data.append(fig)  # Append the entire figure for the company
                       
                    else:
                        stock_data.append({
                            'x': company_df['date'],
                            'y': company_df['value'],
                            'name': company
                        })

            figure = {
                'data': stock_data,
                'layout': {
                    'yaxis': {'type': 'linear' if yaxis_type == 'Linear' else 'log'}  # Set y-axis type in layout
                }
            }

            # Set xaxis type based on radio button selection
            #figure.update_yaxes(type='linear' if yaxis_type == 'Linear' else 'log')
            return figure
        except Exception as e:
            return {'data': []}  # Return empty data in case of errors
    return {}  # Return empty figure initially

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