# **Options Analytics Tool**
## Overview
The Options Analytics Tool is a Python-based project designed to analyze options trading data and calculate option Greeks and implied volatility (IV) values using the Black-Scholes model. The project automates the process of downloading daily market data, combining it, performing analytics, and uploading the results to a SQL database.

## Features
Downloads daily Bhavcopy of cash, F&O, and equity data.
Combines the downloaded data for further analysis.
Utilizes the Black-Scholes model to calculate option Greeks (Delta, Gamma, Theta, Vega, Rho) and IV values.
Generates payoff charts for option strategies.
Uploads the analyzed data to a SQL database for storage and retrieval.
## Project Structure
### constants.py: Contains constant values used throughout the project.
### database_connection.py: Manages connections to the SQL database and handles data uploads.
### main.py: Orchestrates the main workflow of the project, including data downloading, processing, analysis, and uploading.
### model.py: Implements the Black-Scholes model for calculating option prices, Greeks, and IV.
### option_strategy.py: Defines option trading strategies and provides functions for analyzing these strategies.
### option_strategy_payoffs.py: Calculates the payoffs for various option trading strategies.
### payoff_charts.py: Generates payoff charts for visualizing option strategy payoffs.
## Usage
### Setup: Ensure Python and required dependencies are installed (listed in requirements.txt).
Configuration: Set up database connection details in database_connection.py.
Execution: Run main.py to start the analysis process.
Results: Analyzed data, calculated Greeks, IV values, and payoff charts will be available for further analysis or visualization.
Dependencies
Python 3.x
Libraries listed in requirements.txt
Contributors
[Dhiraj Solanki]


Acknowledgements
The Options Analytics Tool utilizes the Black-Scholes model for options pricing, which was developed by Fischer Black and Myron Scholes.
Special thanks to [Ronak Monndra sir and Trading Campus ltd.)
