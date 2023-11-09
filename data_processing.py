import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

# Load the Titanic.csv dataset
titanic_data = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic_data.append(dict(r))

# Load the Players.csv dataset
players_data = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players_data.append(dict(r))

# Load the Teams.csv dataset
teams_data = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams_data.append(dict(r))


class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None


import copy


class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def __is_float(self, element):
        if element is None:
            return False
        try:
            float(element)
            return True
        except ValueError:
            return False

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temps.append(float(item1[aggregation_key]))
            else:
                temps.append(item1[aggregation_key])
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def pivot_table(self, keys_to_pivot_list, keys_to_aggreagte_list, aggregate_func_list):
        # First create a list of unique values for each key
        unique_values_list = []

        # Here is an example of of unique_values_list for
        # keys_to_pivot_list = ['embarked', 'gender', 'class']
        # unique_values_list =
        # [['Southampton', 'Cherbourg', 'Queenstown'], ['M', 'F'], ['3', '2',
        # '1']]

        # Get the combination of unique_values_list
        # You will make use of the function you implemented in Task 2

        import combination_gen

        # code that makes a call to combination_gen.gen_comb_list

        # Example output:
        # [['Southampton', 'M', '3'],
        #  ['Cherbourg', 'M', '3'],
        #  ...
        #  ['Queenstown', 'F', '1']]

        # code that filters each combination

        # for each filter table applies the relevant aggregate functions
        # to keys to aggregate
        # the aggregate functions is listed in aggregate_func_list
        # to keys to aggregate is listed in keys_to_aggreagte_list

        # return a pivot table

        unique_values_list = []

        # Get unique values for each key in keys_to_pivot_list
        for key in keys_to_pivot_list:
            unique_values = list(set(item[key] for item in self.table))
            unique_values_list.append(unique_values)

        print(unique_values_list)

        # Generate combinations of unique values using gen_comb_list
        import combination_gen
        combinations = combination_gen.gen_comb_list(unique_values_list)

        print(combinations)

        # Initialize a dictionary to store pivot table data
        pivot_table_data = []

        # Loop through each combination and filter the table
        for combination in combinations:
            filter_table = self
            for i, key in enumerate(keys_to_pivot_list):
                filter_table = filter_table.filter(lambda x: x[key] == combination[i])

            # Apply aggregate functions to keys to aggregate
            pivot_key = tuple(combination)
            pivot_table_data = []
            for i, key in enumerate(keys_to_aggreagte_list):
                for func in aggregate_func_list:
                    if key in pivot_table_data:
                        pivot_table_data.append(filter_table.select([key]))
                    else:
                        pivot_table_data.append(filter_table.select([key]))

        # Return the pivot table
        # print(pivot_table_data)
        return pivot_table_data

    def __str__(self):
        return self.table_name + ':' + str(self.table)


table1 = Table('cities', cities)
table2 = Table('countries', countries)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_table1 = my_DB.search('cities')

print("Test filter: only filtering out cities in Italy")
my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
print(my_table1_filtered)
print()

print("Test select: only displaying two fields, city and latitude, for cities in Italy")
my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
print(my_table1_selected)
print()

print("Calculting the average temperature without using aggregate for cities in Italy")
temps = []
for item in my_table1_filtered.table:
    temps.append(float(item['temperature']))
print(sum(temps)/len(temps))
print()

print("Calculting the average temperature using aggregate for cities in Italy")
print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
print()

print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
my_table2 = my_DB.search('countries')
my_table3 = my_table1.join(my_table2, 'country')
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
print(my_table3_filtered.table)
print()
print("Selecting just three fields, city, country, and temperature")
print(my_table3_filtered.select(['city', 'country', 'temperature']))
print()

print("Print the min and max temperatures for cities in EU that do not have coastlines")
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
print()

print("Print the min and max latitude for cities in every country")
for item in my_table2.table:
    my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
    if len(my_table1_filtered.table) >= 1:
        print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
print()


# Create tables for the new datasets and insert them into the database
table3 = Table('Titanic', titanic_data)
table4 = Table('Players', players_data)
table5 = Table('Teams', teams_data)

my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)

# First
# Filter players based on the given conditions
filtered_players = table4.filter(lambda x: 'ia' in x['team'] and int(x['minutes']) < 200 and int(x['passes']) > 100)

# Select and display the player surname, team, and position
selected_players = filtered_players.select(['surname', 'team', 'position'])
print("Players :")
print(selected_players)
print()


# Second
# Filter teams based on their ranking
teams_below_10 = table5.filter(lambda x: int(x['ranking']) < 10)
teams_10_and_above = table5.filter(lambda x: int(x['ranking']) >= 10)

# Calculate the average number of games played for each group of teams
avg_games_below_10 = teams_below_10.aggregate(lambda x: sum(x) / len(x), 'games')
avg_games_10_and_above = teams_10_and_above.aggregate(lambda x: sum(x) / len(x), 'games')

print(f"Average games played for teams below 10: {avg_games_below_10}")
print(f"Average games played for teams 10 and above: {avg_games_10_and_above}")
print()


# Third
# Filter players based on their positions
forwards = table4.filter(lambda x: x['position'] == 'forward')
midfielders = table4.filter(lambda x: x['position'] == 'midfielder')

# Calculate the average number of passes for each group of players
avg_passes_forwards = forwards.aggregate(lambda x: sum(x) / len(x), 'passes')
avg_passes_midfielders = midfielders.aggregate(lambda x: sum(x) / len(x), 'passes')

print(f"Average passes made by forwards: {avg_passes_forwards}")
print(f"Average passes made by midfielders: {avg_passes_midfielders}")
print()


# fourth
# Filter passengers in the first class
first_class_passengers = table3.filter(lambda x: x['class'] == '1')

# Filter passengers in the third class
third_class_passengers = table3.filter(lambda x: x['class'] == '3')

# Calculate the average fare for each class
avg_fare_first_class = first_class_passengers.aggregate(lambda x: sum(x) / len(x), 'fare')
avg_fare_third_class = third_class_passengers.aggregate(lambda x: sum(x) / len(x), 'fare')

print(f"Average fare paid by passengers in first class: {avg_fare_first_class}")
print(f"Average fare paid by passengers in third class: {avg_fare_third_class}")
print()


# fifth
# Filter male passengers
male_passengers = table3.filter(lambda x: x['gender'] == 'M')

# Filter female passengers
female_passengers = table3.filter(lambda x: x['gender'] == 'F')

# Calculate the survival rate for each gender
male_survived = [x for x in male_passengers.table if x['survived'] == 'yes']
female_survived = [x for x in female_passengers.table if x['survived'] == 'yes']

print(f"Survival number of male passengers: {len(male_survived)}")
print(f"Survival number of female passengers: {len(female_survived)}")
print(f"The survival rate of male versus female passengers : {len(male_survived) / len(female_survived)}")
print()

table4 = Table('titanic', titanic_data)
my_DB.insert(table4)
my_table4 = my_DB.search('titanic')
my_pivot = my_table4.pivot_table(['embarked', 'gender', 'class'],
                                 ['fare', 'fare', 'fare', 'last'],
                                 [lambda x: min(x), lambda x: max(x),
                                  lambda x: sum(x)/len(x), lambda x: len(x)])

