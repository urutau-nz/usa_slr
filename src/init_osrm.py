import subprocess
import os


## ADD SILENCE VARIABLE

def main(config, logger, update = False, silence = True):
    ''' run the shell script that
    - removes the existing docker
    - downloads the osrm files
    - establishes the osrm routing docker
    '''
    logger.error('Initialize the OSRM server for {} to {} in {}'.format(
        config['transport_mode'], config['services'], config['location']['city']))
    # transport mode options
    mode_dict = {'driving': 'car', 'walking': 'foot',
                    'cycling': 'bicycle', 'access': 'access'}

    # pull the variables from the config file
    osm_subregion = config['OSM']['osm_subregion']
    osm_region = config['OSM']['osm_region']
    port = config['OSRM']['port']
    transport_mode = mode_dict[config['transport_mode']]
    directory = config['OSM']['data_directory']
    state = config['location']['state']

    # in shell, remove any existing dockers
    shell_commands = [
        'docker stop osrm-{}-{}'.format(state, transport_mode),
        'docker rm osrm-{}-{}'.format(state, transport_mode),
    ]
    for com in shell_commands:
        subprocess.run(com.split())

    # download the data
    #download_data = 'wget -N https://download.geofabrik.de/{}/{}-latest.osm.pbf -P {}'.format(osm_region, osm_subregion, directory)
    # p = subprocess.run(download_data.split(), stderr=subprocess.PIPE, bufsize=0)
    # compile_osrm = '304 Not Modified' not in str(p.stderr)
    compile_osrm = True  # True  #

    # if the data does not redownload, it does not need to re-compile.
    if compile_osrm:
        if update:
            logger.error('Compiling the data files & updating network')
            shell_commands = [
                # init docker data
                'docker run -t -v {}:/data osrm/osrm-backend osrm-extract -p /data/profiles/{}.lua /data/{}-latest.osm.pbf'.format(
                    directory, transport_mode, osm_subregion),
                'docker run -t -v {}:/data osrm/osrm-backend osrm-partition /data/{}-latest.osrm'.format(
                    directory, osm_subregion),
                'docker run -t -v {}:/data osrm/osrm-backend osrm-customize /data/{}-latest.osrm --segment-speed-file /data/updates/update.csv'.format(
                    directory, osm_subregion),
            ]
            for com in shell_commands:
                if silence:
                    subprocess.run(com.split(), stdout=open(os.devnull, 'wb'))
                else:
                    subprocess.run(com.split())
        else:
            logger.error('Compiling the data files')
            shell_commands = [
                # init docker data
                'docker run -t -v {}:/data osrm/osrm-backend osrm-extract -p /data/profiles/{}.lua /data/{}-latest.osm.pbf'.format(
                    directory, transport_mode, osm_subregion),
                'docker run -t -v {}:/data osrm/osrm-backend osrm-partition /data/{}-latest.osrm'.format(
                    directory, osm_subregion),
                'docker run -t -v {}:/data osrm/osrm-backend osrm-customize /data/{}-latest.osrm'.format(
                    directory, osm_subregion),
            ]
            for com in shell_commands:
                if silence:
                    subprocess.run(com.split(), stdout=open(os.devnull, 'wb'))
                else:
                    subprocess.run(com.split())
    else:
        logger.error(
            'Data not re-downloaded and compiled because no changes to online version')

    run_docker = 'docker run -d --name osrm-{}-{} -t -i -p {}:5000 -v {}:/data osrm/osrm-backend osrm-routed --algorithm mld --max-table-size 100000 /data/{}-latest.osrm'.format(
        state, transport_mode, port, directory, osm_subregion)
    subprocess.run(run_docker.split())

    logger.error('OSRM server initialized')
