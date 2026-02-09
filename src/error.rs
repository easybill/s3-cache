pub enum ApplicationError {
    Io(std::io::Error),
    Otlp(opentelemetry_otlp::ExporterBuildError),
    Internal(String),
}

impl std::error::Error for ApplicationError {}

impl std::fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            ApplicationError::Io(io_error) => write!(f, "IO error: {io_error:?}"),
            ApplicationError::Otlp(otlp_error) => write!(f, "Otlp error: {otlp_error:?}"),
            Self::Internal(message) => write!(f, "Internal error: {message}"),
        }
    }
}

impl std::fmt::Debug for ApplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl From<std::io::Error> for ApplicationError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<opentelemetry_otlp::ExporterBuildError> for ApplicationError {
    fn from(value: opentelemetry_otlp::ExporterBuildError) -> Self {
        Self::Otlp(value)
    }
}
