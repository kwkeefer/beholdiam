import csv

class CSVParser():
    def __init__(self):
        pass

    def single_column_csv_to_list(self, string):
        """ Takes a CSV string from a CSV which has only one column.
        Converts this string into a list, omitting the header column. """

        reader = csv.reader(string.splitlines())
        list_output = []
        for row in reader:
            list_output.extend(row)
        list_output.pop(0)
        return list_output

    def csv_to_list_of_dicts(self, string):
        """ Takes a CSV string.  Creates a list of dictionaries by
        using the headers of the CSV as keys, and the values in each row as values. """

        reader = csv.DictReader(string.splitlines())
        list_of_dicts = []
        for row in reader:
            list_of_dicts.append(dict(row))
        return list_of_dicts
