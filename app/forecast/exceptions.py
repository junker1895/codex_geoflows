class ForecastError(Exception):
    """Base forecast domain exception."""


class ForecastValidationError(ForecastError):
    """Raised when caller input is structurally invalid."""


class ProviderBackendUnavailableError(ForecastError):
    """Raised when a provider capability is unavailable in current mode/environment."""


class ProviderOperationalError(ForecastError):
    """Raised when provider operation fails despite valid input/capabilities."""
