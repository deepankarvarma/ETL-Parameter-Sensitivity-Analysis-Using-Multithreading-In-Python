# ETL System with Parameter Sensitivity Analysis

This project contains an extract, transform, load (ETL) system implemented in Python. The system extracts data from CSV files, transforms it as required, and loads it into a database.

The system has been designed to perform parameter sensitivity analysis. Specifically, it plots the execution time of different file sizes as a parameter sensitivity and plots the number of records in the particular file with the total time taken for the process.

## Getting Started

To get started with this project, you'll need to clone the repository to your local machine.

```
git clone https://github.com/yourusername/etl-system.git
```

### Prerequisites

To run the ETL system, you'll need to have Python 3.x and the following libraries installed:

- pandas
- matplotlib

You can install these libraries using pip:

```
pip install pandas matplotlib
```

### Usage

To use the ETL system, you'll need to modify the `extract`, `transform`, and `load` functions in the `etl_system.py` file to match the data you're working with and the database you're loading the data into. You'll also need to modify the `files` list to include the names of the files you're working with.

Once you've made the necessary modifications, you can run the `etl_system.py` file:

```
python etl_system.py
```

This will execute the ETL system for each file in the `files` list and plot the results.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

This project was inspired by the need to perform parameter sensitivity analysis on ETL systems. Special thanks to the Pandas and Matplotlib teams for providing the libraries we used to implement this system.