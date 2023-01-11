## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features. We recognize it's a bit unclean to define these in multiple places, but at this point it's the only workaround if you'd like your custom conn type to show up in the Airflow UI.
def get_provider_info():
    return {
        "package-name": "airflow-provider-sqream-blue",
        "name": "Sqream blue Airflow Provider",
        "description": "About Apache Airflow - A platform to programmatically author, schedule, and monitor workflows",
        "hook-class-names": ["sqream_blue.hooks.sqream_blue.SQreamBlueHook"],
        "versions": ["0.0.1"]
    }