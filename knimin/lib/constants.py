from collections import defaultdict
survey_type = {
    1: "Human",
    2: "Animal",
    3: "Fermented foods",
    4: "Surfers",
    5: "Personal microbiome"
}

# Columns to remove for EBI or other public submissions of metadata
ebi_remove = ['ABOUT_YOURSELF_TEXT', 'ANTIBIOTIC_CONDITION', 'ANTIBIOTIC_MED',
              'BIRTH_MONTH', 'CAT_CONTACT', 'CAT_LOCATION',
              'CONDITIONS_MEDICATION', 'DIET_RESTRICTIONS_LIST', 'DOG_CONTACT',
              'DOG_LOCATION', 'GENDER', 'MEDICATION_LIST',
              'OTHER_CONDITIONS_LIST', 'PREGNANT_DUE_DATE', 'RACE_OTHER',
              'RELATIONSHIPS_WITH_OTHERS_IN_STUDY', 'SPECIAL_RESTRICTIONS',
              'SUPPLEMENTS', 'TRAVEL_LOCATIONS_LIST', 'ZIP_CODE',
              'WILLING_TO_BE_CONTACTED']
# standard fields that are set based on sampling site
env_lookup = {
    'Animal Habitat':
    {'SAMPLE_GROUP': 'animal habitat',
     'SCIENTIFIC_NAME': 'metagenome',
     'TAXON_ID': '256318',
     'ENV_MATERIAL': 'animal-associated habitat',
     'ENV_PACKAGE': 'host-associated',
     'ENV_FEATURE': 'animal-associated habitat',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project animal habitat sample',
     'SAMPLE_TYPE': 'animal-associated'
     },
    'Biofilm':
    {'SAMPLE_GROUP': 'biofilm',
     'SCIENTIFIC_NAME': 'biofilm metagenome',
     'TAXON_ID': '718308',
     'ENV_MATERIAL': 'organic material',
     'ENV_PACKAGE': 'microbial mat/biofilm',
     'ENV_FEATURE': 'biofilm',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project biofilm sample',
     'SAMPLE_TYPE': 'biofilm'
     },
    'Dust':
    {'SAMPLE_GROUP': 'indoor',
     'SCIENTIFIC_NAME': 'Dust metagenome',
     'TAXON_ID': '1236744',
     'ENV_MATERIAL': 'dust',
     'ENV_PACKAGE': 'built-environment',
     'ENV_FEATURE': 'environmental material',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project dust sample',
     'SAMPLE_TYPE': 'dust'
     },
    'Food':
    {'SAMPLE_GROUP': 'food',
     'SCIENTIFIC_NAME': 'food metagenome',
     'TAXON_ID': '870726',
     'ENV_MATERIAL': 'food product',
     'ENV_PACKAGE': 'misc environment',
     'ENV_FEATURE': 'anthropogenic environmental material',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project food sample',
     'SAMPLE_TYPE': 'food'
     },
    'Fermented Food':
    {'SAMPLE_GROUP': 'food',
     'SCIENTIFIC_NAME': 'Food fermentation metagenome',
     'TAXON_ID': '1154581',
     'ENV_MATERIAL': 'fermented food product',
     'ENV_PACKAGE': 'misc environment',
     'ENV_FEATURE': 'anthropogenic environmental material',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project fermented food sample',
     'SAMPLE_TYPE': 'food'
     },
    'Indoor Surface':
    {'SAMPLE_GROUP': 'indoor',
     'SCIENTIFIC_NAME': 'Indoor metagenome',
     'TAXON_ID': '1256227',
     'ENV_MATERIAL': 'dust',
     'ENV_PACKAGE': 'built environment',
     'ENV_FEATURE': 'building',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project indoor surface sample',
     'SAMPLE_TYPE': 'dust'
     },
    'Outdoor Surface':
    {'SAMPLE_GROUP': 'outdoor',
     'SCIENTIFIC_NAME': 'ecological metagenome',
     'TAXON_ID': '410657',
     'ENV_MATERIAL': 'surface layer',
     'ENV_PACKAGE': 'misc environment',
     'ENV_FEATURE': 'building',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project outdoor surface sample',
     'SAMPLE_TYPE': 'surface'
     },
    'Plant habitat':
    {'SAMPLE_GROUP': 'outdoor',
     'SCIENTIFIC_NAME': 'plant metagenome',
     'TAXON_ID': '1297885',
     'ENV_MATERIAL': 'organic material',
     'ENV_PACKAGE': 'plant-associated',
     'ENV_FEATURE': 'environmental material',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project plant habitat sample',
     'SAMPLE_TYPE': 'plant'
     },
    'Soil':
    {'SAMPLE_GROUP': 'outdoor',
     'SCIENTIFIC_NAME': 'soil metagenome',
     'TAXON_ID': '410658',
     'ENV_MATERIAL': 'soil',
     'ENV_PACKAGE': 'soil',
     'ENV_FEATURE': 'soil',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project soil sample',
     'SAMPLE_TYPE': 'soil'
     },
    'Sole of shoe':
    {'SAMPLE_GROUP': 'indoor',
     'SCIENTIFIC_NAME': 'Dust metagenome',
     'TAXON_ID': '1236744',
     'ENV_MATERIAL': 'dust',
     'ENV_PACKAGE': 'built-environment',
     'ENV_FEATURE': 'anthropogenic environmental material',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project dust sample',
     'SAMPLE_TYPE': 'dust'
     },
    'Water':
    {'SAMPLE_GROUP': 'water',
     'SCIENTIFIC_NAME': 'freshwater metagenome',
     'TAXON_ID': '449393',
     'ENV_MATERIAL': 'water',
     'ENV_PACKAGE': 'water',
     'ENV_FEATURE': 'environmental material',
     'ENV_BIOME': 'dense settlement biome',
     'DESCRIPTION': 'American Gut Project water sample',
     'SAMPLE_TYPE': 'water'
     }
}

md_lookup = {
    'Hair':
        {'BODY_PRODUCT': 'UBERON:sebum',

         'SAMPLE_TYPE': 'Hair',
         'SCIENTIFIC_NAME': 'human skin metagenome',
         'TAXON_ID': '539655',
         'BODY_HABITAT': 'UBERON:hair',
         'ENV_MATERIAL': 'sebum',
         'ENV_PACKAGE': 'human-associated',
         'DESCRIPTION': 'American Gut Project Hair sample',
         'BODY_SITE': 'UBERON:hair'},
    'Nares': {
        'BODY_PRODUCT': 'UBERON:mucus',
        'SAMPLE_TYPE': 'Nares',
        'SCIENTIFIC_NAME': 'human nasal/pharyngeal metagenome',
        'TAXON_ID': '1131769',
        'BODY_HABITAT': 'UBERON:nose',
        'ENV_MATERIAL': 'mucus',
        'ENV_PACKAGE': 'human-skin',
        'DESCRIPTION': 'American Gut Project Nares sample',
        'BODY_SITE': 'UBERON:nostril'},
    'Vaginal mucus': {
        'BODY_PRODUCT': 'UBERON:mucus',
        'SAMPLE_TYPE': 'Vaginal mucus',
        'SCIENTIFIC_NAME': 'human vaginal metagenome',
        'TAXON_ID': '1632839',
        'BODY_HABITAT': 'UBERON:vagina',
        'ENV_MATERIAL': 'mucus',
        'ENV_PACKAGE': 'human-vaginal',
        'DESCRIPTION': 'American Gut Project Vaginal mucus sample',
        'BODY_SITE': 'UBERON:vaginal introitus'},
    'Sole of foot': {
        'BODY_PRODUCT': 'UBERON:sebum',
        'SAMPLE_TYPE': 'Sole of foot',
        'SCIENTIFIC_NAME': 'human skin metagenome',
        'TAXON_ID': '539655',
        'BODY_HABITAT': 'UBERON:skin',
        'ENV_MATERIAL': 'sebum',
        'ENV_PACKAGE': 'human-skin',
        'DESCRIPTION': 'American Gut Project Sole of foot sample',
        'BODY_SITE': 'UBERON:skin of foot'},
    'Nasal mucus': {
        'BODY_PRODUCT': 'UBERON:mucus',
        'SAMPLE_TYPE': 'Mucus',
        'SCIENTIFIC_NAME': 'human nasal/pharyngeal metagenome',
        'TAXON_ID': '1131769',
        'BODY_HABITAT': 'UBERON:nose',
        'ENV_MATERIAL': 'mucus',
        'ENV_PACKAGE': 'human-associated',
        'DESCRIPTION': 'American Gut Project Nasal mucus sample',
        'BODY_SITE': 'UBERON:nostril'},
    'Stool': {
        'BODY_PRODUCT': 'UBERON:feces',
        'SAMPLE_TYPE': 'Stool',
        'SCIENTIFIC_NAME': 'human gut metagenome',
        'TAXON_ID': '408170',
        'BODY_HABITAT': 'UBERON:feces',
        'ENV_MATERIAL': 'feces',
        'ENV_PACKAGE': 'human-gut',
        'DESCRIPTION': 'American Gut Project Stool sample',
        'BODY_SITE': 'UBERON:feces'},
    'Forehead': {
        'BODY_PRODUCT': 'UBERON:sebum',
        'SAMPLE_TYPE': 'Forehead',
        'SCIENTIFIC_NAME': 'human skin metagenome',
        'TAXON_ID': '539655',
        'BODY_HABITAT': 'UBERON:skin',
        'ENV_MATERIAL': 'sebum',
        'ENV_PACKAGE': 'human-skin',
        'DESCRIPTION': 'American Gut Project Forehead sample',
        'BODY_SITE': 'UBERON:skin of head'},
    'Tears': {
        'BODY_PRODUCT': 'UBERON:tears',
        'SAMPLE_TYPE': 'Tears',
        'SCIENTIFIC_NAME': 'human eye metagenome',
        'TAXON_ID': '1774142',
        'BODY_HABITAT': 'UBERON:eye',
        'ENV_MATERIAL': 'tears',
        'ENV_PACKAGE': 'human-associated',
        'DESCRIPTION': 'American Gut Project Tears sample',
        'BODY_SITE': 'UBERON:eye'},
    'Right hand': {
        'BODY_PRODUCT': 'UBERON:sebum',
        'SAMPLE_TYPE': 'Right Hand',
        'SCIENTIFIC_NAME': 'human skin metagenome',
        'TAXON_ID': '539655',
        'BODY_HABITAT': 'UBERON:skin',
        'ENV_MATERIAL': 'sebum',
        'ENV_PACKAGE': 'human-skin',
        'DESCRIPTION': 'American Gut Project Right Hand sample',
        'BODY_SITE': 'UBERON:skin of hand'},
    'Torso': {
        'BODY_PRODUCT': 'UBERON:sebum',
        'SAMPLE_TYPE': 'Torso',
        'SCIENTIFIC_NAME': 'human skin metagenome',
        'TAXON_ID': '539655',
        'BODY_HABITAT': 'UBERON:skin',
        'ENV_MATTER': 'sebum',
        'DESCRIPTION': 'American Gut Project torso sample',
        'BODY_SITE': 'UBERON:skin of trunk'},
    'Left leg': {
        'BODY_PRODUCT': 'UBERON:sebum',
        'SAMPLE_TYPE': 'Left leg',
        'SCIENTIFIC_NAME': 'human skin metagenome',
        'TAXON_ID': '539655',
        'BODY_HABITAT': 'UBERON:skin',
        'ENV_MATTER': 'sebum',
        'DESCRIPTION': 'American Gut Project left leg sample',
        'BODY_SITE': 'UBERON:skin of leg'},
    'Right leg': {
        'BODY_PRODUCT': 'UBERON:sebum',
        'SAMPLE_TYPE': 'Right leg',
        'SCIENTIFIC_NAME': 'human skin metagenome',
        'TAXON_ID': '539655',
        'BODY_HABITAT': 'UBERON:skin',
        'ENV_MATTER': 'sebum',
        'DESCRIPTION': 'American Gut Project right leg sample',
        'BODY_SITE': 'UBERON:skin of leg'},
    'Mouth': {
        'BODY_PRODUCT': 'UBERON:saliva',
        'SAMPLE_TYPE': 'Mouth',
        'SCIENTIFIC_NAME': 'human oral metagenome',
        'TAXON_ID': '447426',
        'BODY_HABITAT': 'UBERON:oral cavity',
        'ENV_MATERIAL': 'saliva',
        'ENV_PACKAGE': 'human-oral',
        'DESCRIPTION': 'American Gut Project Mouth sample',
        'BODY_SITE': 'UBERON:tongue'},
    'Left hand': {
        'BODY_PRODUCT': 'UBERON:sebum',
        'SAMPLE_TYPE': 'Left Hand',
        'SCIENTIFIC_NAME': 'human skin metagenome',
        'TAXON_ID': '539655',
        'BODY_HABITAT': 'UBERON:skin',
        'ENV_MATERIAL': 'sebum',
        'ENV_PACKAGE': 'human-skin',
        'DESCRIPTION': 'American Gut Project Left Hand sample',
        'BODY_SITE': 'UBERON:skin of hand'},
    'Ear wax': {
        'BODY_PRODUCT': 'UBERON:cerumen',
        'SAMPLE_TYPE': 'Ear wax',
        'SCIENTIFIC_NAME': 'human metagenome',
        'TAXON_ID': '646099',
        'BODY_HABITAT': 'UBERON:ear',
        'ENV_MATERIAL': 'ear wax',
        'ENV_PACKAGE': 'human-associated',
        'DESCRIPTION': 'American Gut Project Ear wax sample',
        'BODY_SITE': 'UBERON:external auditory meatus'}
}

month_int_lookup = {'January': 1, 'February': 2, 'March': 3,
                    'April': 4, 'May': 5, 'June': 6,
                    'July': 7, 'August': 8, 'September': 9,
                    'October': 10, 'November': 11, 'December': 12}

month_str_lookup = {1: 'January', 2: 'February', 3: 'March',
                    4: 'April', 5: 'May', 6: 'June',
                    7: 'July', 8: 'August', 9: 'September',
                    10: 'October', 11: 'November', 12: 'December'}

season_lookup = {None: 'Unspecified',
                 1: 'Winter',
                 2: 'Winter',
                 3: 'Spring',
                 4: 'Spring',
                 5: 'Spring',
                 6: 'Summer',
                 7: 'Summer',
                 8: 'Summer',
                 9: 'Fall',
                 10: 'Fall',
                 11: 'Fall',
                 12: 'Winter'}

# The next two dictionaries are adapted from information presented in
#       Wikipedia. "List of regions of the United States". updated 7 June 2014,
#           accessed 7 June 2014.
#           http://en.wikipedia.org/wiki/List_of_regions_of_the_United_States

regions_by_state = {None: {'Census_1': 'Unspecified',
                           'Census_2': 'Unspecified',
                           'Economic': 'Unspecified'},
                    'AK': {'Census_1': 'West',
                           'Census_2': 'Pacific',
                           'Economic': 'Far West'},
                    'AL': {'Census_1': 'South',
                           'Census_2': 'East South Central',
                           'Economic': 'Southeast'},
                    'AR': {'Census_1': 'South',
                           'Census_2': 'West South Central',
                           'Economic': 'Southeast'},
                    'AZ': {'Census_1': 'West',
                           'Census_2': 'Mountain',
                           'Economic': 'Southwest'},
                    'CA': {'Census_1': 'West',
                           'Census_2': 'Pacific',
                           'Economic': 'Far West'},
                    'CO': {'Census_1': 'West',
                           'Census_2': 'Mountain',
                           'Economic': 'Rocky Mountain'},
                    'CT': {'Census_1': 'Northeast',
                           'Census_2': 'New England',
                           'Economic': 'New England'},
                    'DC': {'Census_1': 'South',
                           'Census_2': 'South Atlantic',
                           'Economic': 'Mideast'},
                    'DE': {'Census_1': 'Northeast',
                           'Census_2': 'Mid-Atlantic',
                           'Economic': 'Mideast'},
                    'FL': {'Census_1': 'South',
                           'Census_2': 'South Atlantic',
                           'Economic': 'Southeast'},
                    'GA': {'Census_1': 'South',
                           'Census_2': 'South Atlantic',
                           'Economic': 'Southeast'},
                    'HI': {'Census_1': 'West',
                           'Census_2': 'Pacific',
                           'Economic': 'Far West'},
                    'IA': {'Census_1': 'Midwest',
                           'Census_2': 'West North Central',
                           'Economic': 'Plains'},
                    'ID': {'Census_1': 'West',
                           'Census_2': 'Mountain',
                           'Economic': 'Rocky Mountain'},
                    'IL': {'Census_1': 'Midwest',
                           'Census_2': 'East North Central',
                           'Economic': 'Great Lakes'},
                    'IN': {'Census_1': 'Midwest',
                           'Census_2': 'East North Central',
                           'Economic': 'Great Lakes'},
                    'KS': {'Census_1': 'Midwest',
                           'Census_2': 'West North Central',
                           'Economic': 'Plains'},
                    'KY': {'Census_1': 'South',
                           'Census_2': 'East South Central',
                           'Economic': 'Southeast'},
                    'LA': {'Census_1': 'South',
                           'Census_2': 'West South Central',
                           'Economic': 'Southeast'},
                    'MA': {'Census_1': 'Northeast',
                           'Census_2': 'New England',
                           'Economic': 'New England'},
                    'MD': {'Census_1': 'South',
                           'Census_2': 'South Atlantic',
                           'Economic': 'Mideast'},
                    'ME': {'Census_1': 'Northeast',
                           'Census_2': 'New England',
                           'Economic': 'New England'},
                    'MI': {'Census_1': 'Midwest',
                           'Census_2': 'East North Central',
                           'Economic': 'Great Lakes'},
                    'MN': {'Census_1': 'Midwest',
                           'Census_2': 'West North Central',
                           'Economic': 'Plains'},
                    'MO': {'Census_1': 'Midwest',
                           'Census_2': 'West North Central',
                           'Economic': 'Plains'},
                    'MS': {'Census_1': 'South',
                           'Census_2': 'East South Central',
                           'Economic': 'Southeast'},
                    'MT': {'Census_1': 'West',
                           'Census_2': 'Mountain',
                           'Economic': 'Rocky Mountain'},
                    'NC': {'Census_1': 'South',
                           'Census_2': 'South Atlantic',
                           'Economic': 'Southeast'},
                    'ND': {'Census_1': 'Midwest',
                           'Census_2': 'West North Central',
                           'Economic': 'Plains'},
                    'NE': {'Census_1': 'Midwest',
                           'Census_2': 'West North Central',
                           'Economic': 'Plains'},
                    'NH': {'Census_1': 'Northeast',
                           'Census_2': 'New England',
                           'Economic': 'New England'},
                    'NJ': {'Census_1': 'Northeast',
                           'Census_2': 'Mid-Atlantic',
                           'Economic': 'Mideast'},
                    'NM': {'Census_1': 'West',
                           'Census_2': 'Mountain',
                           'Economic': 'Southwest'},
                    'NV': {'Census_1': 'West',
                           'Census_2': 'Mountain',
                           'Economic': 'Far West'},
                    'NY': {'Census_1': 'Northeast',
                           'Census_2': 'Mid-Atlantic',
                           'Economic': 'Mideast'},
                    'OH': {'Census_1': 'Midwest',
                           'Census_2': 'East North Central',
                           'Economic': 'Great Lakes'},
                    'OK': {'Census_1': 'South',
                           'Census_2': 'West South Central',
                           'Economic': 'Southwest'},
                    'OR': {'Census_1': 'West',
                           'Census_2': 'Pacific',
                           'Economic': 'Far West'},
                    'PA': {'Census_1': 'Northeast',
                           'Census_2': 'Mid-Atlantic',
                           'Economic': 'Mideast'},
                    'PR': {'Census_1': 'Territories',
                           'Census_2': 'Territories',
                           'Economic': 'Territories'},
                    'RI': {'Census_1': 'Northeast',
                           'Census_2': 'New England',
                           'Economic': 'New England'},
                    'SC': {'Census_1': 'South',
                           'Census_2': 'South Atlantic',
                           'Economic': 'Southeast'},
                    'SD': {'Census_1': 'Midwest',
                           'Census_2': 'West North Central',
                           'Economic': 'Plains'},
                    'TN': {'Census_1': 'South',
                           'Census_2': 'East South Central',
                           'Economic': 'Southeast'},
                    'TX': {'Census_1': 'South',
                           'Census_2': 'West South Central',
                           'Economic': 'Southwest'},
                    'UT': {'Census_1': 'West',
                           'Census_2': 'Mountain',
                           'Economic': 'Rocky Mountain'},
                    'VA': {'Census_1': 'South',
                           'Census_2': 'South Atlantic',
                           'Economic': 'Southeast'},
                    'VI': {'Census_1': 'Territories',
                           'Census_2': 'Territories',
                           'Economic': 'Territories'},
                    'VT': {'Census_1': 'Northeast',
                           'Census_2': 'New England',
                           'Economic': 'New England'},
                    'WA': {'Census_1': 'West',
                           'Census_2': 'Pacific',
                           'Economic': 'Far West'},
                    'WI': {'Census_1': 'Midwest',
                           'Census_2': 'East North Central',
                           'Economic': 'Great Lakes'},
                    'WV': {'Census_1': 'South',
                           'Census_2': 'South Atlantic',
                           'Economic': 'Southeast'},
                    'WY': {'Census_1': 'West',
                           'Census_2': 'Mountain',
                           'Economic': 'Rocky Mountain'}}


def default_blank():
    return 'Not applicable'

blanks_values = defaultdict(default_blank,
                            ASSIGNED_FROM_GEO="No",
                            COMMON_NAME="unclassified metagenome",
                            COUNTRY="USA",
                            ELEVATION='193.0',
                            ENV_BIOME="urban biome",
                            ENV_FEATURE="research facility",
                            ENV_MATERIAL="sterile water",
                            ENV_PACKAGE='misc environment',
                            TAXON_ID='256318',
                            LATITUDE='32.8',
                            LONGITUDE='-117.2',
                            GEO_LOC_NAME='USA:CA:San Diego',
                            PUBLIC='Yes',
                            SAMPLE_TYPE='control blank',
                            SCIENTIFIC_NAME='metagenome',
                            STATE='CA',
                            TITLE='American Gut Project',
                            DNA_EXTRACTED='TRUE',
                            HOST_TAXID='256318',
                            PHYSICAL_SPECIMEN_LOCATION='UCSDMI',
                            PHYSICAL_SPECIMEN_REMAINING='FALSE',
                            DESCRIPTION='American Gut control',
                            SUBSET_AGE=str(False),
                            SUBSET_DIABETES=str(False),
                            SUBSET_IBD=str(False),
                            SUBSET_ANTIBIOTIC_HISTORY=str(False),
                            SUBSET_BMI=str(False),
                            SUBSET_HEALTHY=str(False))
