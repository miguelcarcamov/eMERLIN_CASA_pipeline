# Dependencies
import os,sys,math
import numpy as np
import pickle
import getopt
import logging
import collections
import json

# CASA imports
from taskinit import *
from tasks import *
import casadef


current_version = 'v1.0.10'

# Find path of pipeline to find external files (like aoflagger strategies or emerlin-2.gif)
try:
    pipeline_filename = sys.argv[sys.argv.index('-c') + 1]
    pipeline_path = os.path.abspath(os.path.dirname(pipeline_filename))
except:
    pass

if pipeline_path[-1] != '/':
    pipeline_path = pipeline_path + '/'
sys.path.append(pipeline_path)

import functions.eMCP_functions as em
import functions.eMCP_weblog as emwlog
import functions.eMCP_plots as emplt

casalog.setlogfile('casa_eMCP.log')

# Functions
# Save and load dictionaries
def save_obj(obj, name):
    with open(name, 'wb') as f:
        pickle.dump(obj, f)

def load_obj(name):
    with open(name, 'rb') as f:
        return pickle.load(f)

def deunicodify_hook(pairs):
    # Solves the problem of using unicode in python 2 when strings are needed
    # and uses ordered dict
    new_pairs = []
    for key, value in pairs:
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        new_pairs.append((key, value))
    return collections.OrderedDict(new_pairs)


def get_pipeline_version(pipeline_path):
    headfile = pipeline_path + '.git/HEAD'
    branch = open(headfile, 'rb').readlines()[0].strip().split('/')[-1]
    commit = open(pipeline_path + '.git/refs/heads/'+branch, 'rb').readlines()[0].strip()
    short_commit = commit[:7]
    return branch, short_commit


def create_dir_structure(pipeline_path):
    # Paths to use
    weblog_dir = './weblog/'
    info_dir   = './weblog/info/'
    calib_dir  = './weblog/calib/'
    plots_dir  = './weblog/plots/'
    logs_dir   = './logs/'
    images_dir = './weblog/images/'

    ## Create directory structure ##
    em.makedir(weblog_dir)
    em.makedir(info_dir)
    em.makedir(plots_dir)
    em.makedir(calib_dir)
    em.makedir(images_dir)
    em.makedir(logs_dir)
    em.makedir(plots_dir+'caltables')
    os.system('cp -p {0}/utils/emerlin-2.gif {1}'.format(pipeline_path, weblog_dir))
    os.system('cp -p {0}/utils/eMCP.css {1}'.format(pipeline_path, weblog_dir))
    return calib_dir, info_dir

def start_eMCP_dict(info_dir):
    try:
        eMCP = load_obj(info_dir + 'eMCP_info.pkl')
    except:
        eMCP = collections.OrderedDict()
        eMCP['steps'] = em.eMCP_info_start_steps()
        eMCP['img_stats'] = collections.OrderedDict()
    return eMCP

def get_logger(
        LOG_FORMAT     = '%(asctime)s | %(levelname)s | %(message)s',
        DATE_FORMAT    = '%Y-%m-%d %H:%M:%S',
        LOG_NAME       = 'logger',
        LOG_FILE_INFO  = 'eMCP.log'):

    log           = logging.getLogger(LOG_NAME)
    log_formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
    logging.Formatter.converter = time.gmtime

    # comment this to suppress console output    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)
    log.addHandler(stream_handler) 

    # Normal eMCP log with all information
    file_handler_info = logging.FileHandler(LOG_FILE_INFO, mode='a')
    file_handler_info.setFormatter(log_formatter)
    file_handler_info.setLevel(logging.INFO)
    log.addHandler(file_handler_info)

    log.setLevel(logging.INFO)
    return log


def run_pipeline(inputs=None, inputs_path=''):
    #Create directory structure
    calib_dir, info_dir = create_dir_structure(pipeline_path)

    # Setup logger
    logger = get_logger()

    # Initialize eMCP dictionary, or continue with previous pipeline configuration if possible:
    eMCP = start_eMCP_dict(info_dir)
    eMCP['inputs'] = inputs

    try:
        branch, short_commit = get_pipeline_version(pipeline_path)
    except:
        branch, short_commit = 'unknown', 'unknown'
    pipeline_version = current_version
    logger.info('Starting pipeline')
    logger.info('Running pipeline from:')
    logger.info('{}'.format(pipeline_path))
    logger.info('CASA version: {}'.format(casadef.casa_version))
    logger.info('Pipeline version: {}'.format(pipeline_version))
    logger.info('Using github branch: {}'.format(branch))
    logger.info('github last commit: {}'.format(short_commit))
    logger.info('This log uses UTC times')
    eMCP['pipeline_path'] = pipeline_path
    eMCP['casa_version'] = casadef.casa_version
    em.check_pipeline_conflict(eMCP, pipeline_version)
    eMCP['pipeline_version'] = pipeline_version
    save_obj(eMCP, info_dir + 'eMCP_info.pkl')
    # Inputs
    if inputs_path == '': # Running pipeline
        inputs = em.check_in(pipeline_path)
    else: # Running pipeline from within CASA
        inputs = em.headless(inputs_path)

    # Load default parameters
    if os.path.isfile('./default_params.json'):
        defaults_file = './default_params.json'
    else:
        defaults_file = pipeline_path+'/default_params.json'
    logger.info('Loading default parameters from {0}:'.format(defaults_file))
    eMCP['defaults'] = json.loads(open(defaults_file).read(),
                                  object_pairs_hook=deunicodify_hook)


    #################################
    ### LOAD AND PREPROCESS DATA  ###
    #################################

    ## Pipeline processes, inputs are read from the inputs dictionary
    if inputs['run_importfits'] > 0:
        eMCP = em.import_eMERLIN_fitsIDI(eMCP)

    if os.path.isdir('./'+inputs['inbase']+'.ms') == True:
        msfile = inputs['inbase']+'.ms'
        eMCP, msinfo, msfile = em.get_msinfo(eMCP, msfile)
        em.plot_elev_uvcov(eMCP)

    ### check for parallelisation
    if os.path.isdir('./'+inputs['inbase']+'.mms') == True:
        msfile = inputs['inbase']+'.mms'
        eMCP, msinfo, msfile = em.get_msinfo(eMCP, msfile)
        em.plot_elev_uvcov(eMCP)

    ### Run AOflagger
    if inputs['flag_aoflagger'] > 0:
        eMCP = em.run_aoflagger_fields(eMCP)

    ### A-priori flagdata: Lo&Mk2, edge channels, standard quack
    if inputs['flag_apriori'] > 0:
        eMCP = em.flagdata1_apriori(eMCP)

    ### Load manual flagging file
    if inputs['flag_manual'] > 0:
        eMCP = em.flagdata_manual(eMCP, run_name='flag_manual')

    ### Average data ###
    if inputs['average'] > 0:
        eMCP = em.run_average(eMCP)

    # Check if averaged data already generated
    if os.path.isdir('./'+inputs['inbase']+'_avg.mms') == True:
        msfile = './'+inputs['inbase']+'_avg.mms'
        eMCP, msinfo, msfile = em.get_msinfo(eMCP, msfile)
        em.plot_elev_uvcov(eMCP)
    elif os.path.isdir('./'+inputs['inbase']+'_avg.ms') == True:
        msfile = './'+inputs['inbase']+'_avg.ms'
        eMCP, msinfo, msfile = em.get_msinfo(eMCP, msfile)
        em.plot_elev_uvcov(eMCP)

    ### Produce some plots ###
    if inputs['plot_data'] == 1:
        eMCP = emplt.make_4plots(eMCP, datacolumn='data')

    ### Save flag status up to this point
    if inputs['save_flags'] == 1:
        eMCP = em.saveflagstatus(eMCP)

    ###################
    ### CALIBRATION ###
    ###################

    ### Initialize caltable dictionary
    caltables = em.initialize_cal_dict(inputs, eMCP)

    ### Restore flag status at to this point
    if inputs['restore_flags'] == 1:
        eMCP = em.restoreflagstatus(eMCP)

    ### Load manual flagging file
    if inputs['flag_manual_avg'] == 1:
        eMCP = em.flagdata_manual(eMCP, run_name='flag_manual_avg')
        caltables['Lo_dropout_scans'] = eMCP['msinfo']['Lo_dropout_scans']
        save_obj(caltables, calib_dir+'caltables.pkl')

    ### Initialize models ###
    if inputs['init_models'] > 0:  # Need to add parameter to GUI
        eMCP = em.run_initialize_models(eMCP)

    ### Initial BandPass calibration ###
    if inputs['bandpass'] > 0:
        eMCP, caltables = em.initial_bp_cal(eMCP, caltables)

    ### Initial gaincal = delay, p, ap ###
    if inputs['initial_gaincal'] > 0:
        eMCP, caltables = em.initial_gaincal(eMCP, caltables)

    ### Flux scale ###
    if inputs['fluxscale'] > 0:
        eMCP, caltables = em.eM_fluxscale(eMCP, caltables)

    ### BandPass calibration with spectral index information ###
    if inputs['bandpass_final'] > 0:
        eMCP, caltables = em.bandpass_final(eMCP, caltables)

    ### Amplitude calibration including spectral information ###
    if inputs['gaincal_final'] > 0:
        eMCP, caltables = em.gaincal_final(eMCP, caltables)

    ### Apply calibration  ###
    if inputs['applycal_all'] > 0:
        eMCP = em.applycal_all(eMCP, caltables)

    ### RFLAG automatic flagging ###
    if inputs['flag_target'] > 0:
        em.run_flag_target(eMCP)

    ### Produce some visibility plots ###
    if inputs['plot_corrected'] > 0:
        eMCP = emplt.make_4plots(eMCP, datacolumn='corrected')

    ### First images ###
    if inputs['first_images'] > 0:
        eMCP = em.run_first_images(eMCP)

    ### Split fields ###
    if inputs['split_fields'] > 0:
        eMCP = em.run_split_fields(eMCP)


    # Keep important files
    save_obj(eMCP, info_dir + 'eMCP_info.pkl')
    os.system('cp eMCP.log {}eMCP.log.txt'.format(info_dir))
    os.system('cp casa_eMCP.log {}casa_eMCP.log.txt'.format(info_dir))

    emwlog.start_weblog(eMCP)

    try:
        os.system('mv casa-*.log *.last ./logs')
        logger.info('Moved casa-*.log *.last to ./logs')
    except:
        pass
    logger.info('Pipeline finished')
    logger.info('#################')

    return



# The  __name__ == "__main__" approach does not work for CASA.
try:
    if run_in_casa == True:
        # Running the pipeline from inside CASA
        print('Pipeline initialized. To run the pipeline within CASA use:')
        print('run_pipeline(inputs_path=<input file>)')
except:
    inputs = em.check_in(pipeline_path)
    run_pipeline(inputs=inputs)

