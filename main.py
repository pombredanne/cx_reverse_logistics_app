"""Main Entry Point to Reverse Logistics Application."""
# Standard Library Imports
import sys

# Local Application Imports
from configs import configs
from pipelines.transformer.pipeline import extract_looker
from pipelines.transformer.pipeline import extract_airtable
from pipelines.transformer.pipeline import transform_looker
from pipelines.transformer.pipeline import transform_to_insert
from pipelines.transformer.pipeline import load_insert_atb
from pipelines.transformer.pipeline import load_insert_sfl

from utils.logging_setup import logging_setup

"""
   Created At: 2019/09/16
      Contact: data@amaro.com
   Created By: gabriela.leticia@amaro.com
Last Reviewed: 
  Reviewed by: 
Copyright © 2012-2019 AMARO. All rights reserved.
"""

log = logging_setup()

def main(argv):

    stage, env = argv[1], argv[2]
    config = configs.get_configs(stage, env)

    e_loo = extract_looker(config)
    e_atb = extract_airtable(config)

    t_loo = transform_looker(e_loo)
    t_ins = transform_to_insert(t_loo, e_atb)

    if not t_ins.empty:
      l_ins_atb = load_insert_atb(config, t_ins)
      load_insert_sfl(config, l_ins_atb)
    else:
        log.info("No Inserts in Airtable.")



if __name__ == "__main__":
    main(sys.argv)
