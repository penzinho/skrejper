# Apify base image with Python + Playwright and all three browser engines
# (chromium, firefox, webkit) preinstalled. Meinestadt rotates across engines,
# so we need them all.
FROM apify/actor-python-playwright:3.12

# Install Python dependencies first to leverage Docker layer caching.
COPY requirements.txt ./

RUN echo "Python version:" \
 && python --version \
 && echo "Pip version:" \
 && pip --version \
 && echo "Installing dependencies:" \
 && pip install -r requirements.txt \
 && echo "All installed Python packages:" \
 && pip freeze

# Copy the rest of the source code.
COPY . ./

# Compile to fail fast on syntax errors at build time.
RUN python -m compileall -q src

# Run the actor (src/__main__.py).
CMD ["python", "-m", "src"]
