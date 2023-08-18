import os
# HeuDiConv heuristics

# Based on: https://github.com/nipy/heudiconv/blob/master/heudiconv/heuristics/example.py
POPULATE_INTENDED_FOR_OPTS = {
        'matching_parameters': ["ModalityAcquisitionLabel"], #['ImagingVolume'],
        'criterion': 'Closest'
}

def create_key(template, outtype=('nii.gz',), annotation_classes=None):
    if template is None or not template:
        raise ValueError('Template must be a valid format string')
    return template, outtype, annotation_classes

def infotodict(seqinfo):
    """Heuristic evaluator for determining which runs belong where
    allowed template fields - follow python string module:
    item: index within category
    subject: participant id
    seqitem: run number during scanning
    subindex: sub index within group
    """

    #---------anat-----------#
    T1w = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:01d}_T1w')
    
    T2w = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:01d}_T2w')
    FLAIR = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:01d}_FLAIR')

    #---------SWI-----------#
    SWI = create_key('sub-{subject}/{session}/swi/sub-{subject}_{session}_run-{item:01d}_swi')
    SWIMag = create_key('sub-{subject}/{session}/swi/sub-{subject}_{session}_run-{item:01d}_part-mag_swi')
    SWIPhase = create_key('sub-{subject}/{session}/swi/sub-{subject}_{session}_run-{item:01d}_part-phase_swi')

    #---------dwi-----------#
    dkiFOR = create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_dir-AP_run-{item:01d}_dwi')
    dkiREV = create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_dir-PA_run-{item:01d}_dwi')
    
    #---------fmap-----------#
    fmapMag = create_key('sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-bold_run-{item:01d}_magnitude')
    fmapPhase = create_key('sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-bold_run-{item:01d}_phasediff')

    #---------perf-----------#
    asl = create_key('sub-{subject}/{session}/perf/sub-{subject}_{session}_run-{item:01d}_asl')

    #---------func-----------#
    bold = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-rest_run-{item:01d}_bold')

    # info dict to be populated
    info = {
            T1w: [], 
            T2w: [], FLAIR: [], 
            SWI:[], SWIMag:[], SWIPhase:[], 
            dkiFOR: [], dkiREV: [], 
            bold: [], 
            fmapMag: [],
            fmapPhase: [],
            asl: []
           }
    
    ##########################################################################################################
    ## This is typically what you will have to change based on your scanner protocols
    ## Use heudiconv stage_1 output file:dicominfo.tsv from all subjects to identify all possible protocol names
    ##########################################################################################################

    keys_protocols_dict = {
        T1w: ['MPRAGE GRAPPA2'],
        
        T2w: ["t2_tse_tra_512"],
        FLAIR: ["t2_space_dark-fluid_sag_p2_ns-t2prep"],        
       
        "T2_SWI": ["t2_swi_tra_p2_2mm"],

        asl: ["pcasl_3d_singleTI"],

        bold: ['MB_ep2d_bold_s8'],

        dkiFOR: ['Diffusion_Kurtosis_FW_S2_modifide'], 
        dkiREV: ['Diffusion_Kurtosis_FW_S2_modifide_rev'], 

        "fmap": ["Field_Mapping"]
        
    }
    # These protcols needs special naming based on image type (see below)
    protocols_with_mag_and_phase = {
                                    "fmap": [fmapMag, fmapPhase],
                                    }

    protocols_with_swi = {
                        "T2_SWI": [SWI,SWIMag,SWIPhase]
                        }

    ##########################################################################################################

    data = create_key('run{item:03d}')
    last_run = len(seqinfo)
    for idx, s in enumerate(seqinfo):
        print(s)
        for key,protocols in keys_protocols_dict.items():
            print(f"key: {key}, protocols: {protocols}")
            for ptcl in protocols:
                if (ptcl in s.protocol_name):    
                    if key in protocols_with_mag_and_phase.keys():
                        if 'M' in s.image_type:
                            new_key = protocols_with_mag_and_phase[key][0] # first entry is mag
                        else:
                            new_key = protocols_with_mag_and_phase[key][1] # second entry is phase

                        info[new_key].append(s.series_id)
                    elif key in protocols_with_swi.keys():
                        if s.series_description == 't2_swi_tra_p2_2mm_SWI':
                            new_key = protocols_with_swi[key][0] # first entry is swi
                        elif s.series_description == 't2_swi_tra_p2_2mm_Mag':
                            new_key = protocols_with_swi[key][1] # second entry is mag
                        else:
                            new_key = protocols_with_swi[key][2] # third entry is phase

                        info[new_key].append(s.series_id)

                    else:                   
                        info[key].append(s.series_id)
                
    return info
