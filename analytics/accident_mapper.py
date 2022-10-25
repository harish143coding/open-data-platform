import pandas as pd
import folium
import scrapper

# default parameters
rytle_street = "Konsul-Smidt-Straße"
csl_street = "Universitätsallee"
bucket_name = "smarthelm-data-accumulation-csl"

# def data_maker(folder_path, bucket_name):
#     with open("total_accidents.csv", "w") as output_file:
#         filenames = scrapper.scrap(folder_path, bucket_name)
#         for file_name in filenames:
#             with open(file_name) as input_file:
#                 contents = input_file.read()
#                 output_file.write(contents)
#     return output_file


# the "total_accidents.csv" hold already accidents from 2019 to 21



def get_the_street(accidents_file_path, street: str) -> pd.DataFrame:
    "Fn to obtain the desired street as dataframe, in our case rytle street"
    accidents_df = pd.read_table(accidents_file_path)
    sample_rytle_street_df = accidents_df.groupby(by="Strasse").get_group(street)
    return sample_rytle_street_df


rytle = folium.Map(location=[53.087465, 8.781670], tiles='OpenStreetMap', zoom_start=14)


def create_accidents_marker(street: pd.DataFrame):
    """
    a street w ith accident information give a dataframe outputs the accident locations as markers
    """
    rytle_geo_col_names = ['Koord.y', 'Koord.x']
    geo_coords = street[rytle_geo_col_names].replace(
        to_replace={"Koord.y": ',', "Koord.x": ','},
        value=".",
        regex=True)
    geo_coords = geo_coords.to_dict(orient='records')
    for x in geo_coords:
        folium.Circle(
            radius=8,
            location=[x['Koord.y'], x['Koord.x']],
            popup="Leicht verletzt",
            color="crimson",
            fill=True,
        ).add_to(rytle)
    return rytle.save("test.html")


if __name__ == "__main__":
    accidents_file_path = data_maker(input("enter the folder path in Minio example  accidents_data/bremen: "), bucket_name )
    street_df = get_the_street(accidents_file_path, rytle_street)
    create_accidents_marker(street_df)
