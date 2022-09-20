import folium
from os.path import exists
import pandas as pd


ACCIDENTS_DATA_PATH = "./data/total_accidents.csv"
DEFAULT_RYTLE = folium.Map(location=[53.087465, 8.781670],  tiles='OpenStreetMap', zoom_start=14)


def accidents_reader(filenames):
    """

    Args:
        filenames: a 'list' containing all the filenames of previous years accident data.

    Returns:

    """
    if exists(ACCIDENTS_DATA_PATH):
        print("accidents data path exists")
        return ACCIDENTS_DATA_PATH
    else:
        with open(ACCIDENTS_DATA_PATH, "w") as output_file:
            for file_name in filenames:
                with open(file_name) as input_file:
                    contents = input_file.read()
                    output_file.write(contents)
        return ACCIDENTS_DATA_PATH




def create_accidents_marker(street_name: pd.DataFrame):
    """
    a street w ith accident information give a dataframe outputs the accident locations as markers
    """
    accidents_df = pd.read_table(ACCIDENTS_DATA_PATH)
    desired_street = accidents_df.groupby(by="Strasse").get_group(street_name)
    rytle_geo_col_names = ['Koord.y', 'Koord.x']
    geo_coords = desired_street[rytle_geo_col_names].replace(
                                    to_replace = {"Koord.y": ',', "Koord.x" : ',' },
                                    value = ".",
                                    regex = True)
    geo_coords = geo_coords.to_dict(orient='records')
    for x in geo_coords:
        folium.Circle(
        radius=8,
        location=[x['Koord.y'], x['Koord.x']],
        popup="Leicht verletzt",
        color="crimson",
        fill=True,
        ).add_to(DEFAULT_RYTLE)
    return DEFAULT_RYTLE.save("test.html")

