
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
# 'enum' for RP's agent staging output types
RP_ASI_NAME_DEFAULT = "DEFAULT"


# ==============================================================================
#
class AgentStagingOutputComponent(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.Component.__init__(self, rpc.AGENT_STAGING_OUTPUT_COMPONENT, cfg)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Stager
    #
    @classmethod
    def create(cls, cfg):

        name = cfg.get('agent_staging_output_component', RP_ASI_NAME_DEFAULT)

        # Make sure that we are the base-class!
        if cls != AgentStagingOutputComponent:
            raise TypeError("Factory only available to base class!")

        from .default import Default

        try:
            impl = {
                RP_ASI_NAME_DEFAULT: Default
            }[name]

            impl = impl(cfg)
            return impl

        except KeyError:
            raise ValueError("AgentStagingOutputComponent '%s' unknown or defunct" % name)


# ------------------------------------------------------------------------------

