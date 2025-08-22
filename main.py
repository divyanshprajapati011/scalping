import streamlit as st
import pandas as pd
import mysql.connector
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time, urllib.parse, re
import hashlib
import io
import requests
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, WebDriverException

st.set_page_config(page_title="Google Maps Scraper with Login (MySQL)", layout="wide")

# ================== DATABASE SETUP ==================
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",       # change if different
        password="",       # add password if set
        database="pythondb"
    )

# ================== SECURITY HELPERS ==================
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def register_user(username, password, email):
    db = get_connection()
    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO users (username, password, email) VALUES (%s,%s,%s)",
                       (username, hash_password(password), email))
        db.commit()
        return True
    except:
        return False
    finally:
        cursor.close()
        db.close()

def login_user(username, password):
    db = get_connection()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM users WHERE username=%s AND password=%s",
                   (username, hash_password(password)))
    result = cursor.fetchone()
    cursor.close()
    db.close()
    return result

def fetch_all_users():
    db = get_connection()
    cursor = db.cursor()
    cursor.execute("SELECT username, email FROM users")
    rows = cursor.fetchall()
    cursor.close()
    db.close()
    return rows

def delete_user(username):
    db = get_connection()
    cursor = db.cursor()
    cursor.execute("DELETE FROM users WHERE username=%s", (username,))
    db.commit()
    cursor.close()
    db.close()

# ================== LOGIN / REGISTER ==================
st.sidebar.title("🔐 User Authentication")
menu = st.sidebar.radio("Menu", ["Login", "Register", "Admin Panel"])

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.user = None

# -------- Register --------
if menu == "Register":
    st.subheader("📝 Create a New Account")
    reg_user = st.text_input("Username")
    reg_email = st.text_input("Email")
    reg_pass = st.text_input("Password", type="password")
    if st.button("Register"):
        if reg_user and reg_email and reg_pass:
            if register_user(reg_user, reg_pass, reg_email):
                st.success("✅ Registered successfully! Please login now.")
            else:
                st.error("❌ Username already exists or DB error.")
        else:
            st.warning("⚠️ Please fill all fields.")

# -------- Login --------
elif menu == "Login":
    st.subheader("🔑 Login to Your Account")
    log_user = st.text_input("Username")
    log_pass = st.text_input("Password", type="password")
    if st.button("Login"):
        user = login_user(log_user, log_pass)
        if user:
            st.session_state.logged_in = True
            st.session_state.user = log_user
            st.success(f"✅ Logged in as {log_user}")

# ================== SCRAPER (only after login) ==================
if st.session_state.get("logged_in"):

    user_input = st.text_input(
        "🔎 Enter query OR Google Search URL OR Google Maps URL",
        "top coaching in Bhopal"
    )

    # Function to normalize input into a clean Google Maps Search URL
    def get_maps_url(user_input: str):
        user_input = user_input.strip()
        if "google.com/search" in user_input and "q=" in user_input:
            m = re.search(r"q=([^&]+)", user_input)
            if m:
                query_text = urllib.parse.unquote(m.group(1))
                return "https://www.google.com/maps/search/" + urllib.parse.quote_plus(query_text)
        elif "google.com/maps" in user_input:
            return user_input
        else:
            return "https://www.google.com/maps/search/" + urllib.parse.quote_plus(user_input)

    maps_url = get_maps_url(user_input)

    max_results = st.number_input("Maximum results to fetch", min_value=5, max_value=500, value=60, step=5)
    do_email_lookup = st.checkbox("Website से Email/extra Phones भी निकालें (slower)", value=True)
    start_btn = st.button("🚀 Start Scraping")

    # ------------------------ Helpers ------------------------
    def setup_driver(headless: bool = True):
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1366,768")
        options.add_argument("--lang=en-US")
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def safe_text(by, sel, ctx=None):
        try:
            el = (ctx or driver).find_element(by, sel)
            return el.text.strip()
        except:
            return ""

    def safe_attr(by, sel, attr="href", ctx=None):
        try:
            el = (ctx or driver).find_element(by, sel)
            return (el.get_attribute(attr) or "").strip()
        except:
            return ""

    def extract_rating_and_reviews(driver):
        rating, reviews = "", ""
        try:
            # Try main rating element
            stars = driver.find_element(By.XPATH, "//span[@role='img' and contains(@aria-label,'stars')]")
            aria = stars.get_attribute("aria-label") or ""
            r1 = re.search(r"(\d+(?:\.\d+)?)", aria)
            rating = r1.group(1) if r1 else ""
            rv = re.search(r"(\d[\d,]*)\s+reviews", aria, re.I)
            reviews = (rv.group(1).replace(",", "") if rv else "")
        except:
            try:
                # Try alternate compact format
                compact = driver.find_element(By.CLASS_NAME, "MW4etd").text.strip()
                if compact:
                    m = re.search(r"(\d+(?:\.\d+)?)", compact)
                    if m: rating = m.group(1)
                    m2 = re.search(r"\((\d[\d,]*)\)", compact)
                    if m2: reviews = m2.group(1).replace(",", "")
            except:
                pass
        return rating, reviews
 

    EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
    PHONE_RE = re.compile(r"(?:\+?\d[\d\-\s]{7,}\d)")
    HEADERS = {"User-Agent": "Mozilla/5.0"}

    def fetch_email_phone_from_site(url, timeout=12):
        if not url:
            return "", ""
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            html = resp.text
            emails = list({e for e in EMAIL_RE.findall(html)})
            phones = list({p.strip() for p in PHONE_RE.findall(html)})
            return "; ".join(emails[:5]), "; ".join(phones[:5])
        except:
            return "", ""

    def scroll_results_panel(driver, hard_limit=500):
        try:
            panel = driver.find_element(By.XPATH, '//div[contains(@aria-label, "Results for")]')
        except NoSuchElementException:
            return
        last_h = driver.execute_script("return arguments[0].scrollHeight", panel)
        stagnant_loops = 0
        while True:
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", panel)
            time.sleep(1.6)
            new_h = driver.execute_script("return arguments[0].scrollHeight", panel)
            if new_h == last_h:
                stagnant_loops += 1
            else:
                stagnant_loops = 0
            last_h = new_h
            cards = driver.find_elements(By.XPATH, "//div[contains(@class,'Nv2PK')]")
            if len(cards) >= hard_limit:
                break
            if stagnant_loops >= 3:
                break

    def scrape_maps(url, limit=60, email_lookup=True):
        global driver
        rows = []
        driver = setup_driver(headless=True)
        try:
            driver.get(url)
            time.sleep(4)
            scroll_results_panel(driver, hard_limit=limit)
            cards = driver.find_elements(By.XPATH, "//div[contains(@class,'Nv2PK')]")
            if not cards:
                cards = [None]

            count = 0
            for idx, card in enumerate(cards):
                if limit and count >= limit:
                    break
                if card is not None:
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'})", card)
                        card.click()
                        time.sleep(2.2)
                    except WebDriverException:
                        continue

                name = safe_text(By.XPATH, '//h1[contains(@class,"DUwDvf")]')
                if not name and card is not None:
                    name = safe_text(By.CLASS_NAME, "qBF1Pd", ctx=card)
                website = safe_attr(By.XPATH, '//a[@data-item-id="authority"]', "href")
                address = safe_text(By.XPATH, '//button[@data-item-id="address"]')
                phone = safe_text(By.XPATH, '//button[starts-with(@data-item-id,"phone:")]')
                rating, reviews = extract_rating_and_reviews(driver)

                if not address:
                    address = safe_text(By.XPATH, "//div[contains(@class,'Io6YTe') and contains(text(),',')]")
                if (not phone) and card is not None:
                    phone = safe_text(By.CLASS_NAME, "UsdlK", ctx=card)

                email_from_site = ""
                extra_phones_from_site = ""
                if email_lookup and website:
                    email_from_site, extra_phones_from_site = fetch_email_phone_from_site(website)

                rows.append({
                    "Business Name": name,
                    "Website": website,
                    "Rating": rating,
                    "Reviews Count": reviews,
                    "Address": address,
                    "Phone (Maps)": phone,
                    "Email (from site)": email_from_site,
                    "Extra Phones (from site)": extra_phones_from_site,
                    "Source (Maps URL)": driver.current_url
                })
                count += 1

            return pd.DataFrame(rows)
        finally:
            try:
                driver.quit()
            except:
                pass

    if start_btn:
        if not maps_url.strip():
            st.error("कृपया Google Maps की search/listing या place URL दें.")
        else:
            with st.spinner("Scraping in progress..."):
                df = scrape_maps(maps_url.strip(), int(max_results), email_lookup=do_email_lookup)

            if df.empty:
                st.warning("कोई डेटा नहीं मिला। URL सही है या नहीं, ये भी चेक कर लें।")
            else:
                st.success(f"Done! {len(df)} rows")
                st.dataframe(df, use_container_width=True)

                csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
                st.download_button("⬇️ Download CSV", data=csv_bytes, file_name="maps_scrape.csv", mime="text/csv")

                out = io.BytesIO()
                with pd.ExcelWriter(out, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False, sheet_name="data")
                st.download_button("⬇️ Download Excel", data=out.getvalue(),
                                   file_name="maps_scrape.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                st.caption("Tip: अधिक results चाहिए तो listing URL दें (search results page), tool auto-scroll से ज़्यादा entries load कर लेगा.")

# -------- Admin Panel --------
elif menu == "Admin Panel":
    st.subheader("🛠️ Admin Panel (Only for Admin User)")
    admin_user = st.text_input("Admin Username")
    admin_pass = st.text_input("Admin Password", type="password")
    if st.button("Login as Admin"):
        if admin_user == "admin" and admin_pass == "admin123":
            st.success("✅ Admin logged in")
            users = fetch_all_users()
            st.write("### Registered Users")
            df_users = pd.DataFrame(users, columns=["Username", "Email"])
            st.dataframe(df_users)

            del_user = st.text_input("Enter username to delete")
            if st.button("Delete User"):
                delete_user(del_user)
                st.warning(f"❌ User '{del_user}' deleted successfully!")
        else:
            st.error("❌ Wrong Admin Credentials")
