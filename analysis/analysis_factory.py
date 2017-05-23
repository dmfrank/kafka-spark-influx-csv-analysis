from analysis.alert_message import AlertMessageFactory
from analysis.analysis import Analysis
from analysis.historical_delivery import HistoricalDataDeliveryFactory


class AnalysisFactory(object):
    """
        Main entry point to analysis module. A AnalysisFactory return instance depending on the configuration.
        It is currently being used all the time one implementation.
    """

    def __init__(self, config, data_structure_after_aggregation):
        """
        :param config: dictionary with configuration parameters. 
        :param data_structure: Structure of the input data after the aggregation operation
        """
        self._historical_data_delivery = HistoricalDataDeliveryFactory(
            config.content["analysis"]).instance_data_delivery()
        self._config = config
        self._data_structure_after_aggregation = data_structure_after_aggregation
        self._data_structure = dict(map(lambda x: reversed(x), enumerate(
            map(lambda x: x["input_field"], filter(lambda x: not x["key"], data_structure_after_aggregation["rule"])))))
        self._alert = AlertMessageFactory(config.content["analysis"]).instance_alert()

    def instance_analysis(self):
        """
        The instance_analysis create instance of class DetectionAnomaly depending on aggregation operation
        :return: 
        """
        return Analysis(self._config.content["analysis"], self._historical_data_delivery, self._alert,
                        self._data_structure_after_aggregation)
