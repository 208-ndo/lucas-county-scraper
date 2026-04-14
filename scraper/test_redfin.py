name: Lucas County Scraper
on:
  schedule:
    - cron: "0 13 * * *"
  workflow_dispatch:
permissions:
  contents: write
jobs:
  scrape-and-deploy:
    runs-on: ubuntu-22.04
    timeout-minutes: 60
    steps:
      - name: Checkout repo
        uses: actions/checkout@v3
        with:
          fetch-depth: 1

      - name: Get week number for cache key
        id: date
        run: echo "week=$(date +%Y-%V)" >> $GITHUB_OUTPUT

      - name: Cache ParcelsAddress DBF
        id: cache-dbf
        uses: actions/cache@v3
        with:
          path: data/parcels
          key: parcels-dbf-${{ steps.date.outputs.week }}
          restore-keys: parcels-dbf-

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install -r scraper/requirements.txt
          pip install dbfread requests
          python -m playwright install --with-deps chromium

      - name: Download ParcelsAddress DBF
        if: steps.cache-dbf.outputs.cache-hit != 'true'
        run: |
          mkdir -p data/parcels
          python scraper/download_parcels.py

      - name: Show parcel data status
        run: |
          if [ -f "data/parcels/ParcelsAddress.dbf" ]; then
            SIZE=$(stat -c%s "data/parcels/ParcelsAddress.dbf")
            echo "DBF found: ${SIZE} bytes"
          elif [ -f "data/parcels/ParcelsAddress.csv" ]; then
            LINES=$(wc -l < "data/parcels/ParcelsAddress.csv")
            echo "CSV fallback: ${LINES} rows"
          else
            echo "No parcel file - using auditor API fallback"
          fi

      - name: Test Redfin API
        run: python scraper/test_redfin.py
        env:
          PYTHONUNBUFFERED: "1"

      - name: Run Toledo scraper
        run: |
          if [ -f "data/parcels/ParcelsAddress.dbf" ]; then
            python scraper/fetch.py --dbf-address "data/parcels/ParcelsAddress.dbf"
          else
            python scraper/fetch.py
          fi
        env:
          PYTHONUNBUFFERED: "1"
          ZILLOW_API_KEY: ${{ secrets.ZILLOW_API_KEY }}

      - name: Commit and push data
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/ dashboard/
          git diff --cached --quiet || git commit -m "Data update $(date -u '+%Y-%m-%d %H:%M UTC')"
          git push

      - name: Deploy to GitHub Pages
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./dashboard
          publish_branch: gh-pages
