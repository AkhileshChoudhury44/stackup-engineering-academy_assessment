import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Dynamic paths to ensure outputs/ lands in repository root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)
pdf_path = os.path.join(OUTPUT_DIR, "dashboard_mockup.pdf")

# Initialize Canvas
fig = plt.figure(figsize=(16, 11), dpi=300)
ax = fig.add_axes([0, 0, 1, 1])
ax.set_xlim(0, 100)
ax.set_ylim(0, 100)
ax.axis("off")

# Background
bg_card = patches.Rectangle((0, 0), 100, 100, facecolor="#F8FAFC", edgecolor="none")
ax.add_patch(bg_card)

# ── Title & Header Banner ───────────────────────────────────────────────────
header_rect = patches.Rectangle((0, 92), 100, 8, facecolor="#0F172A", edgecolor="none")
ax.add_patch(header_rect)
ax.text(2, 96, "PRESIGHT AI — EXECUTIVE PROJECT SPEND & PERFORMANCE DASHBOARD", 
        color="#FFFFFF", fontsize=15, weight="bold")
ax.text(2, 93.5, "Lead Data Engineer: Akhilesh Choudhury  |  Date: 2026-09-06  |  Scope: Enterprise Analytics", 
        color="#94A3B8", fontsize=9)
ax.text(82, 94.5, "STATUS: PRODUCTION", color="#38BDF8", fontsize=10, weight="bold")

# ── Slicers Bar ─────────────────────────────────────────────────────────────
slicer_bar = patches.Rectangle((2, 85.5), 96, 5, facecolor="#FFFFFF", edgecolor="#CBD5E1", lw=1)
ax.add_patch(slicer_bar)

slicers = [
    "Region: [ All Regions v ]", 
    "Project Status: [ Active, In-Progress v ]", 
    "Fiscal Year: [ 2024 - 2026 v ]", 
    "Category: [ All Categories v ]"
]

for i, sl in enumerate(slicers):
    slicer_box = patches.FancyBboxPatch(
        (3.5 + (i * 23.5), 86.5), 22, 3,
        boxstyle="round,pad=0.2",
        facecolor="#F1F5F9",
        edgecolor="#94A3B8",
        lw=0.8
    )
    ax.add_patch(slicer_box)
    ax.text(4.5 + (i * 23.5), 87.8, sl, fontsize=8.5, color="#1E293B", weight="semibold")

# ── KPI Cards ───────────────────────────────────────────────────────────────
kpis = [
    ("TOTAL BUDGET", "AED 48.50M", "Baseline Allocated", "#2563EB"),
    ("TOTAL ACTUAL SPEND", "AED 51.20M", "+AED 2.70M (+5.56% Over)", "#DC2626"),
    ("% OVER-BUDGET PROJECTS", "28.4%", "14 of 49 Active Projects", "#EA580C"),
    ("TOTAL TRANSACTIONS", "1,420", "Avg AED 36,056 / Txn", "#0D9488")
]

for i, (title, val, sub, color) in enumerate(kpis):
    x = 2 + (i * 24.5)
    card = patches.Rectangle((x, 73), 22.5, 10.5, facecolor="#FFFFFF", edgecolor="#E2E8F0", lw=1.2)
    top_strip = patches.Rectangle((x, 82.5), 22.5, 1.0, facecolor=color, edgecolor="none")
    ax.add_patch(card)
    ax.add_patch(top_strip)
    ax.text(x + 1.2, 79.5, title, fontsize=7.5, color="#64748B", weight="bold")
    ax.text(x + 1.2, 76.0, val, fontsize=15, color="#0F172A", weight="bold")
    ax.text(x + 1.2, 74.0, sub, fontsize=7, color=color, weight="semibold")

# ── Visual 1: Bar Chart (Dept Budget vs Spend) ──────────────────────────────
v1_container = patches.Rectangle((2, 40), 54, 31, facecolor="#FFFFFF", edgecolor="#E2E8F0", lw=1.2)
ax.add_patch(v1_container)
ax.text(3.5, 68, "Actual Spend vs Budget by Department (Q1 Results)", fontsize=9.5, weight="bold", color="#0F172A")
ax.text(3.5, 66.2, "Chart: Clustered Bar  |  Why: Direct side-by-side variance analysis per business function", fontsize=7, color="#64748B")

depts = ["Engineering", "Data & AI", "Operations", "Cybersecurity", "Product"]
budgets = [16.2, 12.0, 8.5, 6.8, 5.0]
actuals = [18.1, 13.4, 7.9, 6.9, 4.9]

for idx, d in enumerate(depts):
    y = 61.5 - (idx * 4.4)
    ax.text(3.5, y + 1.2, d, fontsize=7.5, weight="semibold", color="#334155")
    b_bar = patches.Rectangle((17, y + 1.5), budgets[idx] * 1.7, 1.3, facecolor="#94A3B8", edgecolor="none")
    actual_color = "#2563EB" if actuals[idx] <= budgets[idx] else "#EF4444"
    a_bar = patches.Rectangle((17, y), actuals[idx] * 1.7, 1.3, facecolor=actual_color, edgecolor="none")
    ax.add_patch(b_bar)
    ax.add_patch(a_bar)
    ax.text(17 + max(budgets[idx], actuals[idx]) * 1.7 + 1.5, y + 0.6, 
            f"Act: {actuals[idx]}M vs Bud: {budgets[idx]}M", fontsize=6.5, color="#475569")

# ── Visual 2: Donut Chart (Vendor Concentration) ────────────────────────────
v2_container = patches.Rectangle((58, 40), 40, 31, facecolor="#FFFFFF", edgecolor="#E2E8F0", lw=1.2)
ax.add_patch(v2_container)
ax.text(59.5, 68, "Vendor Spend Concentration (Q3 Results)", fontsize=9.5, weight="bold", color="#0F172A")
ax.text(59.5, 66.2, "Chart: Donut  |  Why: Shows Top 5 + 'Other' share and single-supplier exposure", fontsize=7, color="#64748B")

vendors = [
    ("Microsoft Azure", "34.5%", "#1E40AF"), 
    ("Oracle Cloud", "21.2%", "#3B82F6"), 
    ("G42 Injazat", "16.0%", "#60A5FA"), 
    ("Cisco Systems", "11.3%", "#93C5FD"), 
    ("Dell Technologies", "7.0%", "#BFDBFE"), 
    ("Other (18 Vendors)", "10.0%", "#CBD5E1")
]

circle_center = (69, 52.5)
c_outer = patches.Circle(circle_center, 9, facecolor="#F1F5F9", edgecolor="#64748B", lw=1)
c_inner = patches.Circle(circle_center, 5.2, facecolor="#FFFFFF", edgecolor="none")
ax.add_patch(c_outer)
ax.add_patch(c_inner)
ax.text(69, 53.2, "TOP 5", fontsize=9, weight="bold", ha="center", color="#0F172A")
ax.text(69, 51.5, "90% Total", fontsize=7, ha="center", color="#64748B")

for v_idx, (v_name, v_share, v_color) in enumerate(vendors):
    ly = 61.5 - (v_idx * 3.3)
    v_box = patches.Rectangle((81, ly), 1.8, 1.8, facecolor=v_color, edgecolor="none")
    ax.add_patch(v_box)
    ax.text(83.5, ly + 0.3, f"{v_name}: {v_share}", fontsize=7.2, color="#334155", weight="semibold")

# ── Visual 3: Line Chart (Spend Trend by Category) ──────────────────────────
v3_container = patches.Rectangle((2, 3), 48, 35, facecolor="#FFFFFF", edgecolor="#E2E8F0", lw=1.2)
ax.add_patch(v3_container)
ax.text(3.5, 35.5, "Monthly Transaction Spend Trend by Category (Q5)", fontsize=9.5, weight="bold", color="#0F172A")
ax.text(3.5, 33.8, "Chart: Multi-Line  |  Why: Preserves temporal continuity to spot monthly run-rate shifts", fontsize=7, color="#64748B")

ax.plot([6, 46], [8, 8], color="#94A3B8", lw=1)
ax.plot([6, 6], [8, 30], color="#94A3B8", lw=1)

months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
cloud_pts = [10, 12, 15, 17, 22, 28]
hw_pts = [14, 13, 16, 12, 14, 15]
consult_pts = [8, 9, 11, 14, 13, 16]

xs = [6 + (i * 7.5) for i in range(len(months))]
for i, m in enumerate(months):
    ax.text(xs[i], 6.3, m, fontsize=6.8, ha="center", color="#64748B")

ax.plot(xs, cloud_pts, color="#2563EB", lw=2, marker='o')
ax.plot(xs, hw_pts, color="#10B981", lw=1.8, marker='s')
ax.plot(xs, consult_pts, color="#F59E0B", lw=1.8, marker='^')
ax.text(34, 28, "Cloud Infra", fontsize=7, color="#2563EB", weight="bold")
ax.text(34, 15, "Hardware", fontsize=7, color="#10B981", weight="bold")
ax.text(34, 11, "Consulting", fontsize=7, color="#F59E0B", weight="bold")

# ── Visual 4: Table (Top 10 Projects by Variance) ───────────────────────────
v4_container = patches.Rectangle((52, 3), 46, 35, facecolor="#FFFFFF", edgecolor="#E2E8F0", lw=1.2)
ax.add_patch(v4_container)
ax.text(53.5, 35.5, "Top 10 Projects by Budget Variance (projects_clean)", fontsize=9.5, weight="bold", color="#0F172A")
ax.text(53.5, 33.8, "Chart: Ranked Data Grid  |  Why: Provides exact dirham-level audit trails for leadership", fontsize=7, color="#64748B")

headers = ["#", "Project Name", "Dept", "Budget (M)", "Actual (M)", "Variance"]
col_x = [53.5, 56.5, 74.0, 81.5, 87.5, 93.0]

for c_idx, h in enumerate(headers):
    ax.text(col_x[c_idx], 31.5, h, fontsize=7, weight="bold", color="#475569")
ax.plot([53.5, 96.5], [30.5, 30.5], color="#CBD5E1", lw=1)

sample_table = [
    ("1", "Vision Analytics Core", "AI", "4.20", "5.10", "+0.90M"),
    ("2", "Secure Border Gateway", "Cyber", "3.50", "4.15", "+0.65M"),
    ("3", "Data Fabric v2", "Eng", "2.80", "3.35", "+0.55M"),
    ("4", "Autonomous Patrol", "AI", "3.10", "3.52", "+0.42M"),
    ("5", "Smart Traffic Grid", "Eng", "1.90", "2.28", "+0.38M"),
    ("6", "Gov Cloud Enclave", "Infra", "5.00", "5.32", "+0.32M"),
    ("7", "Biometric Terminal", "Cyber", "2.10", "2.35", "+0.25M"),
    ("8", "IoT Sensor Fabric", "Eng", "1.40", "1.60", "+0.20M"),
    ("9", "Video Summarizer", "AI", "1.20", "1.38", "+0.18M"),
    ("10", "Executive BI Portal", "Data", "0.90", "1.05", "+0.15M"),
]

for r_idx, row in enumerate(sample_table):
    ry = 28.0 - (r_idx * 2.45)
    bg = "#F8FAFC" if r_idx % 2 == 0 else "#FFFFFF"
    row_bg = patches.Rectangle((53.0, ry - 0.7), 44.0, 2.3, facecolor=bg, edgecolor="none")
    ax.add_patch(row_bg)
    for c_idx, val in enumerate(row):
        col_color = "#DC2626" if c_idx == 5 else "#1E293B"
        weight = "bold" if c_idx in [1, 5] else "normal"
        ax.text(col_x[c_idx], ry, val, fontsize=6.7, color=col_color, weight=weight)

# Export PDF
plt.savefig(pdf_path, format="pdf", bbox_inches="tight")
plt.close()
print(f"Executive Mockup PDF generated successfully at: {pdf_path}")