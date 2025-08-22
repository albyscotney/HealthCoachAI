import pandas as pd
import logging

logger = logging.getLogger("EnricherPlugins")

class EnrichmentBase:
    """
    A base template for all enrichment classes.
    It ensures that every enrichment has the required properties and methods.
    """
    # The name of the enrichment for logging purposes.
    name = "Base Enrichment"
    # The InfluxDB measurement this enrichment should read from.
    target_measurement = None
    # The name of the new measurement where enriched data will be written.
    output_measurement = None

    def __init__(self):
        """Initializes the enrichment and loads any necessary reference data."""
        self.reference_data = None
        self.load_reference_data()

    def load_reference_data(self):
        """
        Loads a reference dataset from a file (e.g., CSV, JSON).
        This method is called once when the service starts.
        Override this in your custom enrichment class if it needs external data.
        """
        pass

    def enrich(self, point):
        """
        The core logic of the enrichment.
        Takes one data point (as a dict) and returns a dict of new fields.
        This method MUST be implemented by every enrichment class.
        """
        raise NotImplementedError("The 'enrich' method must be implemented by subclasses.")

# ------------------------------------------------------------------------------------
# PLUGIN 1: Calculates Sleep Efficiency (No reference data needed)
# ------------------------------------------------------------------------------------
class SleepEfficiencyEnrichment(EnrichmentBase):
    name = "Sleep Efficiency Calculator"
    target_measurement = "SleepSummary"
    output_measurement = "EnrichedSleepSummary"

    def enrich(self, point):
        fields = point.get('fields', {})
        sleep_time_sec = fields.get('sleepTimeSeconds')
        awake_time_sec = fields.get('awakeSleepSeconds')

        if sleep_time_sec is not None and awake_time_sec is not None:
            total_time_in_bed = sleep_time_sec + awake_time_sec
            if total_time_in_bed > 0:
                efficiency = round((sleep_time_sec / total_time_in_bed) * 100, 2)
                return {"sleepEfficiencyPercent": efficiency}
        return None # Return None if calculation isn't possible

# ------------------------------------------------------------------------------------
# PLUGIN 2: Adds a qualitative judgement to the sleep score using a CSV file
# ------------------------------------------------------------------------------------
class SleepJudgementEnrichment(EnrichmentBase):
    name = "Sleep Score Judgement"
    target_measurement = "SleepSummary"
    output_measurement = "EnrichedSleepSummary"

    def load_reference_data(self):
        """Loads the judgement mapping from our CSV file."""
        try:
            # Pandas is great for this, but you could use the standard 'csv' module too.
            self.reference_data = pd.read_csv("sleep_scores.csv")
            logger.info(f"[{self.name}] Successfully loaded reference data from sleep_scores.csv")
        except FileNotFoundError:
            logger.error(f"[{self.name}] CRITICAL: Could not find reference file 'sleep_scores.csv'. This enrichment will not work.")
            self.reference_data = None

    def enrich(self, point):
        # Don't run if the reference data failed to load
        if self.reference_data is None:
            return None

        fields = point.get('fields', {})
        sleep_score = fields.get('sleepScore')

        if sleep_score is not None:
            for _, row in self.reference_data.iterrows():
                if row['min_score'] <= sleep_score <= row['max_score']:
                    return {"sleepJudgement": row['judgement']}
        return None

# ------------------------------------------------------------------------------------
# ADD ALL YOUR ENRICHMENT CLASSES TO THIS LIST TO MAKE THEM ACTIVE
# ------------------------------------------------------------------------------------
ALL_ENRICHMENTS = [
    SleepEfficiencyEnrichment,
    SleepJudgementEnrichment,
    # Add your next enrichment class here, e.g., ActivityAnalysisEnrichment
]