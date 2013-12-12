import os
import importlib


os.environ.setdefault("OASTATS_SETTINGS", "settings")

class Settings(object):

    def __init__(self):
        settings_module = importlib.import_module(os.environ['OASTATS_SETTINGS'])
        self.SETTINGS = SettingsContainer(settings_module)

    def __getattr__(self, name):
        return getattr(self.SETTINGS, name)


class SettingsContainer(object):

    def __init__(self, settings):
        for setting in dir(settings):
            if setting == setting.upper():
                value = getattr(settings, setting)
                setattr(self, setting, value)


settings = Settings()
