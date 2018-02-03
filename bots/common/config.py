YYYMMDD = "%Y-%m-%d"
DDMMMYYYY = "%d-%b-%Y"
DDMMMYY = "%d-%b-%y"
MMMYYYY = "%b-%Y"
MMMYY = "%b-%y"
DEFAULT_DATE_FORMAT = DDMMMYY
DEFAULT_MMYYYY_FORMAT = MMMYY
START = "start_date_field"
END = "end_date_field"
QUARTER_CONVENTION = {1: 3, 2: 6, 3: 9, 4: 12}
HEADERS = dict()

HEADERS["juniper"] = ['product',
                      'version',
                      'frsDate',
                      'eoEngineering',
                      'eoSupport',
                      'url']

HEADERS["adobe"] = ['product',
                    'version',
                    'build',
                    'availability',
                    'core',
                    'extended']

HEADERS["citrix"] = ['product',
                     'language',
                     'version',
                     'End_of_sales',
                     'End_of_maintenance',
                     'End_of_life',
                     'End_of_extended_support']

HEADERS["nec"] = ['product',
                  'end_of_sale',
                  'end_of_support'
                  ]

HEADERS["puppet"] = ['puppet_enterprise_versions',
                     'start_mainstream_support',
                     'start_limited_support',
                     'end_of_life'
                     ]

HEADERS["kronos"] = ['product',
                     'release',
                     'status',
                     'migration_upgradation_path'
                     ]

HEADERS["opentext"] = ['productname', 'version', 'releasedate', 'sustainingsupport']

HEADERS["tableaeu"] = ['product', 'version', 'release_date']
HEADERS["microstrategy"] = ['version', 'support_status', 'original_release_date', 'expected_expiration_date']
HEADERS["trendmicro"] = ['Product', 'Version', 'Language', 'Platform', 'Region', 'Stop_Support']
HEADERS["bmc"] = ['Category', 'Product', 'Announced', 'End_of_Life']
HEADERS["quest"] = ['product', 'version', 'FullSupport', 'LimitedSupport', 'SupportDiscontinued']

HEADERS["landesk"] = ['Product', 'Available_purchase', 'Full_support_ends', 'CC_support_ends', 'Web_based_support_ends',
                      'Product_availability_support_ends', 'Description']
HEADERS["redhat"] = ['Product', 'Version', 'GeneralAvailability', 'Start_of_Full_Support', 'End_of_Full_Support',
                     'Start_of_Maintenance_Support', 'End_of_Maintenance_Support', 'life_cycle', 'End_of_ELS1',
                     'End_of_ELS2', 'long_life', 'EndofProduction1', 'EndofProduction2', 'CertEnd1',
                     'Migaration_Support', 'End_of_Production3_End_of_Production_Phase',
                     'End_of_Extended_Lifecycle_Support', 'End_of_Extended_LifePhase', 'LastMinorRelease',
                     'Support_Level', 'End_of_Life']
HEADERS["kaspersky"] = ['product', 'release_date', 'patch', 'version', 'curr_status', 'end_of_full_support',
                        'end_of_lim_support', 'end_of_extended_support', 'end_of_partial_updates', 'end_of_life']

HEADERS["mulesoft"] = ['product', 'version', 'release_date', 'end_of_standard_support', 'supported_mule_versions']
HEADERS["microfocus"] = ['name', 'egs', 'ess', 'cur_version', 'replace_prod']
HEADERS["ni"] = ['Product', 'Operating_System', 'Release_Date', 'Language']
HEADERS["hp"] = ['product',
                 'release',
                 'end_of_support_notification',
                 'end_of_support_committed',
                 'end_of_extended_support',
                 'end_of_self_help']
HEADERS["winzip"] = ['product',
                     'version',
                     'release',
                     'eol',
                     'support'
                     ]
