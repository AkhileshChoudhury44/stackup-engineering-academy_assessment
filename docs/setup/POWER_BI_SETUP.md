# Power BI Setup

For **Task 2.4 — Dashboard Design**.

The assessment asks for a one-page executive dashboard. Power BI Desktop is the preferred tool, but we accept several alternatives if Power BI isn't available to you.

---

## Option A — Power BI Desktop (Windows only)

### Install

1. Download Power BI Desktop (free) from:
   https://powerbi.microsoft.com/desktop/

2. Choose the **64-bit installer** (most systems)

3. Run the installer with default options:
   - ✅ Accept license terms
   - ✅ Create desktop shortcut

4. Launch Power BI Desktop — **no Microsoft account is required** for local report building

### Sign in (optional)

You only need to sign in if you want to publish reports to the Power BI Service. For the assessment, local development is fine — you can skip sign-in.

### First-time configuration

1. **File → Options → Global → Preview features**
2. Tick: ☑ **Modern visual tooltips** (better hover info)
3. **File → Options → Current file → Regional settings**
4. Set locale to your region (e.g. English (United Arab Emirates))

---

### Connect to assessment data

Power BI loads data via connectors.

1. Open Power BI Desktop
2. Home tab → **Get Data → Text/CSV**
3. Navigate to `outputs/transactions_clean.csv`
4. Preview window opens — verify columns look correct → **Load**
5. Repeat for:
   - `outputs/projects_clean.csv`
   - `outputs/employees_clean.csv`

> Tip: If your ETL pipeline (Task 2.2) hasn't been run yet, you can load the raw datasets from `datasets/` to start designing. Re-import the cleaned data later.

### Define relationships

1. Click **Model view** (left sidebar, third icon)
2. Drag `transactions.project_id` onto `projects.project_id` to create a relationship
3. Drag `transactions.approved_by` onto `employees.employee_id`
4. Click each relationship line to confirm:
   - Cardinality: **Many to one (*:1)**
   - Cross filter direction: **Single**

### Build the required visuals

Per Task 2.4, your dashboard needs:

| Visual | What to use | Data |
|---|---|---|
| KPI cards | Card visual | Total budget, total actual spend, % over budget projects |
| Bar chart | Clustered bar chart | Actual spend vs budget by department |
| Line chart | Line chart | Monthly transaction spend trend |
| Table | Table visual | Top 5 projects by budget variance |
| Filters | Slicer | Region, project status |

### Recommended measures (DAX)

Create these in the **Modeling** tab → **New measure**:

```dax
Total Budget = SUM(projects[budget])

Total Actual Cost = SUM(projects[actual_cost])

Budget Utilisation % =
DIVIDE([Total Actual Cost], [Total Budget], 0) * 100

Over Budget Projects =
CALCULATE(
    COUNTROWS(projects),
    projects[actual_cost] > projects[budget]
)

Total Transactions = SUM(transactions[amount])
```

### Format and theme

1. **View tab → Themes** → pick a professional theme (Sunset, Tidal, or Executive work well)
2. Add a title text box at the top: "Presight — Project Spend Dashboard"
3. Add your name and the date in small text in the corner

### Save and export

1. **File → Save As → `presight_dashboard.pbix`**
2. Move the file to your repo's `outputs/` folder
3. Commit and push as part of your PR

> File size note: `.pbix` files can be 5–50 MB depending on data. Use Git LFS if it exceeds 100 MB (unlikely for this dataset).

---

## Option B — Power BI Service (web — Mac / Linux friendly)

For Mac and Linux users who can't install Desktop.

### Sign up

1. Go to https://app.powerbi.com
2. Sign up with a **work or school email** (personal Gmail/Outlook won't work)
3. If you don't have a work email, use **Power BI Service free trial** with a Microsoft 365 developer account: https://developer.microsoft.com/microsoft-365/dev-program

### Upload data

1. My workspace → **+ New → Upload a file**
2. **Local File** → upload your cleaned CSVs

### Build visuals in web

The web report builder has fewer features than Desktop but covers the basics for this assessment.

### Export as PDF

1. Open your finished report
2. **File → Export → Export to PDF**
3. Save as `outputs/presight_dashboard.pdf`

---

### Requirements for a mockup submission

Your mockup must:

1. **Show the full dashboard layout** with all five required visual types
2. **Use realistic numbers** — pull actual figures from your dataset analysis
3. **Annotate each visual** with:
   - What data it shows
   - Why you chose that chart type
   - What insight it gives the executive viewer

### Save

- Export as PDF
- Place in `outputs/dashboard_mockup.pdf`

---



