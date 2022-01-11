import plotly.graph_objects as go
import plotly
import numpy as np
import pandas as pd
import itertools
import osmnx as ox
import networkx as nx
import code

def plot_path(lat, long, origin_point, destination_point):
    """
    Given a list of latitudes and longitudes, origin
    and destination point, plots a path on a map
    Parameters
    ----------
    lat, long: list of latitudes and longitudes
    origin_point, destination_point: co-ordinates of origin
    and destination
    Returns
    -------
    Nothing. Only shows the map.
    """
    # adding the lines joining the nodes
    fig = go.Figure(go.Scattermapbox(
        name = "Path",
        mode = "lines",
        lon = long,
        lat = lat,
        marker = {'size': 10},
        line = dict(width = 4.5, color = 'blue')))
    # adding source marker
    fig.add_trace(go.Scattermapbox(
        name = "Source",
        mode = "markers",
        lon = [origin_point[0]],
        lat = [origin_point[1]],
        marker = {'size': 12, 'color':"red"}))
    # adding destination marker
    fig.add_trace(go.Scattermapbox(
        name = "Destination",
        mode = "markers",
        lon = [destination_point[0]],
        lat = [destination_point[1]],
        marker = {'size': 12, 'color':'green'}))
    # getting center for plots:
    lat_center = np.mean(lat)
    long_center = np.mean(long)
    # defining the layout using mapbox_style
    fig.update_layout(mapbox_style="stamen-terrain",
        mapbox_center_lat = 30, mapbox_center_lon=-80)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0},
                      mapbox = {
                          'center': {'lat': lat_center,
                          'lon': long_center},
                          'zoom': 13})
    file_name = input('File Name: ')
    fig.write_image("./fig/{}.png".format(file_name))

lst = input('List of Coordinates: ')
#code.interact(local=locals())
orig = lst[0]
dest = lst[-1]
# lst = list(lst)
lat = []
long = []

for i in np.arange(0,len(lst)):
    lat.append(lst[i][1])
    long.append(lst[i][0])



plot_path(lat, long, orig, dest)



## Try this - get overview - plot route (save fig) - Test again with http://project-osrm.org/docs/v5.24.0/api/#general-options (apparently can add to string options about snapping) - do again with 0ft (so will have 10ft and 0ft) - compare difference to see if snap location changes if nearest road has max_speed=0, or changes under different annotation (see link)
# curl 'http://localhost:6050/table/v1/driving/-87.88063352068428458,30.9708659997810507;-87.59155253099999,30.269993227000043?overview=false&annotations=distance&sources=0&destinations=1'



# curl 'http://localhost:6050/table/v1/driving/-87.88063352068428458,30.9708659997810507;-87.59155253099999,30.269993227000043?overview=true'

# curl 'http://localhost:6051/route/v1/driving/-87.88063352068428458,30.9708659997810507;-87.8335,30.9831?overview=full&geometries=geojson'
# curl 'http://localhost:6051/route/v1/driving/-87.88063352068428458,30.9708659997810507;-87.8335,30.9831?overview=full&geometries=geojson&snapping=any'
# curl 'http://localhost:6050/route/v1/driving/-87.88063352068428458,30.9708659997810507;-87.8335,30.9831?overview=full&geometries=geojson&radiuses=500;250'


'''
Example of how to run this:

Step 1. Set up the osrm docker
Step 2. Generate the address. E.g., curl 'http://localhost:6050/route/v1/driving/-87.88063352068428458,30.9708659997810507;-87.8335,30.9831?overview=full&geometries=geojson'
Step 3. Copy paste the list of coordinates
lst = [[-87.872085,30.970444],[-87.872069,30.970276],[-87.871817,30.969665],[-87.871747,30.969527],[-87.871683,30.969462],[-87.871506,30.969393],[-87.871377,30.969384],[-87.870583,30.96926],[-87.870122,30.969237],[-87.869966,30.9692],[-87.869864,30.969117],[-87.8698,30.969007],[-87.869666,30.968436],[-87.869596,30.96828],[-87.869189,30.96816],[-87.869006,30.968133],[-87.868877,30.968137],[-87.868674,30.968179],[-87.868599,30.968179],[-87.868352,30.96811],[-87.868309,30.968114],[-87.868255,30.968147],[-87.868196,30.968211],[-87.868132,30.968381],[-87.868121,30.96862],[-87.8681,30.968731],[-87.868105,30.968988],[-87.868073,30.96909],[-87.867976,30.96914],[-87.867805,30.96914],[-87.867697,30.969108],[-87.86737,30.968956],[-87.867152,30.968881],[-87.866999,30.968943],[-87.866948,30.96921],[-87.866961,30.969385],[-87.867066,30.969537],[-87.867195,30.969817],[-87.867195,30.969997],[-87.867155,30.97016],[-87.867135,30.970359],[-87.867179,30.970653],[-87.867169,30.970967],[-87.865613,30.971127],[-87.865719,30.972338],[-87.86585,30.973932],[-87.865898,30.974491],[-87.865293,30.974536],[-87.864791,30.974518],[-87.864308,30.974388],[-87.863834,30.974224],[-87.863627,30.974161],[-87.863416,30.974115],[-87.863202,30.974086],[-87.862986,30.974075],[-87.862763,30.974082],[-87.86263,30.974086],[-87.862486,30.974109],[-87.862342,30.974146],[-87.862137,30.974228],[-87.86205,30.974283],[-87.861952,30.974344],[-87.861833,30.974458],[-87.861797,30.974492],[-87.861703,30.974624],[-87.86167,30.974671],[-87.861488,30.975093],[-87.861426,30.975262],[-87.861214,30.975839],[-87.861175,30.97593],[-87.861125,30.976014],[-87.861024,30.976111],[-87.860896,30.976172],[-87.860758,30.976213],[-87.860554,30.976266],[-87.860015,30.976407],[-87.859991,30.976413],[-87.859781,30.976474],[-87.859577,30.976552],[-87.859383,30.976652],[-87.859203,30.976776],[-87.859039,30.976917],[-87.858294,30.977591],[-87.858201,30.977673],[-87.857917,30.977923],[-87.857715,30.978087],[-87.857526,30.978241],[-87.857111,30.978525],[-87.856987,30.978599],[-87.856785,30.978711],[-87.856708,30.978749],[-87.855991,30.979101],[-87.854559,30.979804],[-87.854235,30.979942],[-87.854222,30.979947],[-87.85414,30.979979],[-87.854043,30.980011],[-87.85376,30.980066],[-87.853545,30.980089],[-87.853043,30.980095],[-87.853002,30.980094],[-87.852539,30.980087],[-87.852468,30.980089],[-87.852034,30.980103],[-87.851818,30.980124],[-87.851604,30.980156],[-87.851392,30.980196],[-87.851182,30.980245],[-87.850777,30.980347],[-87.850475,30.980435],[-87.850411,30.980453],[-87.849226,30.980777],[-87.848663,30.980931],[-87.848401,30.981014],[-87.84846,30.981183],[-87.848503,30.981293],[-87.848676,30.981698],[-87.848369,30.981645],[-87.848229,30.981617],[-87.847736,30.981494],[-87.847237,30.981396],[-87.846735,30.981309],[-87.845717,30.981211],[-87.844704,30.981138],[-87.84446,30.981137],[-87.844197,30.981136],[-87.843695,30.981185],[-87.843197,30.981262],[-87.842697,30.981349],[-87.842208,30.981484],[-87.841722,30.981639],[-87.841234,30.981779],[-87.840744,30.981912],[-87.840245,30.982022],[-87.83974,30.9821],[-87.839249,30.982186],[-87.838741,30.98224],[-87.838378,30.982233],[-87.838233,30.98223],[-87.837737,30.982308],[-87.83675,30.982516],[-87.836175,30.982631],[-87.834401,30.982954],[-87.83351,30.982919]]
orig = [-87.88063352068428458,30.9708659997810507]
dest = [-87.8335,30.9831]
'''
