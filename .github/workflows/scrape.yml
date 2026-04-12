name: Scrape Lucas County Leads

on:
  workflow_dispatch:
  schedule:
    - cron: "0 7 * * *"

permissions:
  contents: write
  pages: write
  id-token: write

jobs:
  scrape:
    runs-on: ubuntu-22.04

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests beautifulsoup4 lxml dbfread playwright

      - name: Install Playwright Chromium
        run: python -m playwright install --with-deps chromium

      - name: Run scraper
        run: python scraper/fetch.py

      - name: Commit results
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add dashboard/records.json data/records.json || true
          git commit -m "Update records" || echo "No changes to commit"
          git push

  deploy-pages:
    needs: scrape
    runs-on: ubuntu-22.04
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Configure Pages
        uses: actions/configure-pages@v5

      - name: Upload Pages artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: dashboard

      - name: Deploy to GitHub Pages
        uses: actions/deploy-pages@v4
