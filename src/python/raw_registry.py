'''
Medicaid Domain for NSPAH

Running this module will create/update data model for raw CMS data

https://github.com/NSAPH/data_model

Demographics:   /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-demographics_patient
    Description of columns:
        https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/dominici_data_pipelines/-/blob/master/medicaid/1_create_demographics_data.R
Enrollments:    /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-ps_patient-year
    Description of columns:
        https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/dominici_data_pipelines/-/blob/master/medicaid/2_process_enrollment_data.R
        https://github.com/NSAPH/data_requests/tree/master/request_projects/dec2019_medicaid_platform_cvd
Admissions:     /data/incoming/rce/ci3_health_data/medicaid/cvd/1999_2012/desouza/data
    Description of columns:
        https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/dominici_data_pipelines/-/blob/master/medicaid/code/2_create_cvd_data.R
        https://github.com/NSAPH/data_requests/blob/master/request_projects/dec2019_medicaid_platform_cvd/cvd_readme.md

Sample user request: https://github.com/NSAPH/data_requests/tree/master/request_projects/feb2021_jenny_medicaid_resp
'''

import os
from pathlib import Path

import yaml

from create_schema_config import CMSSchema
from fts2yaml import MedicaidFTS
from nsaph import init_logging


class Registry:
    """
    This class parses File Transfer Summary files and
    creates YAML data model. It can either
    update built-in registry or write
    the model to a designated path
    """

    def __init__(self, context: CMSSchema = None):
        init_logging()
        if not context:
            context = CMSSchema(__doc__).instantiate()
        self.context = context

    def update(self):
        if self.context.output is None:
            registry_path = self.built_in_registry_path()
        else:
            registry_path = self.context.output
        with open(registry_path, "wt") as f:
            f.write(self.create_yaml())
        return

    def create_yaml(self):
        name = "cms"
        domain = {
            name: {
                "reference": "https://resdac.org/getting-started-cms-data",
                "schema": name,
                "index": "explicit",
                "quoting": 3,
                "header": False,
                "tables": {
                }
            }
        }
        for x in ["ps", "ip"]:
            domain[name]["tables"].update(
                MedicaidFTS(x).init(self.context.input).to_dict()
            )
        return yaml.dump(domain)

    def built_in_registry_path(self):
        src = Path(__file__).parents[3]
        return os.path.join(src, "yml", "cms.yaml")


if __name__ == '__main__':
    Registry().update()