FROM devxygmbh/r-ubuntu

# Install system dependencies
# Fix GPG key issues and update package lists
RUN rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* && \
    apt-get clean && \
    apt-get update -o Acquire::AllowInsecureRepositories=true \
                   -o Acquire::AllowDowngradeToInsecureRepositories=true && \
    apt-get install -y --allow-unauthenticated \
    --no-install-recommends \
    libpq-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libsodium-dev \
    curl \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install R packages from binaries
RUN R -e "install.packages('pak')"
# Install remaining packages
RUN R -e "pak::pak(c('dplyr', 'dbplyr', 'stringr', 'lubridate', 'purrr', 'tidyr', 'DBI', 'RPostgres', 'mirai', 'promises', 'optparse', 'fs', 'fabricatr', 'S7', 'randomNames', 'readr', 'here', 'dotenv', 'assertthat'))"

# Set working directory
WORKDIR /app

# Copy package files
COPY R ./R

# Expose port
EXPOSE 8000

# Run the API
CMD ["bash"]