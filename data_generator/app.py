"""
Application Streamlit de génération et d'injection des données de test WABA Group.

Permet de piloter la simulation des flux pour les 8 pays d'Afrique de l'Ouest
et les 4 métiers (Banque, Assurance, Mobile Money, Microfinance) avec envoi vers MinIO.

Auteur : Armel Kouadio
"""

import argparse
import datetime
import os
import sys
import time
from typing import List

# Ensure parent directory is in Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generator.generator_core import WabaDataGenerator
from data_generator.minio_uploader import MinioUploader
from data_generator.schemas import (
    COUNTRIES,
    CURRENCY_MAP,
    ENTITY_TYPES,
)


def run_streamlit_app():
    """Renders the Streamlit Interactive UI."""
    import streamlit as st

    st.set_page_config(
        page_title="WABA Group - Data Lakehouse Generator",
        page_icon="🏦",
        layout="wide"
    )

    st.title("🏦 WestAfrica BancAssur Group (WABA)")
    st.subheader("Générateur de Transactions & Ingestion MinIO (Level 1)")
    st.markdown(
        """
        Console d'émission des flux pour les **8 filiales ouest-africaines**
        du groupe WABA, garantissant l'intégrité référentielle stricte et le respect des devises locales.
        """
    )

    # Initialize session state for persistent generator and minio client
    if "generator" not in st.session_state:
        st.session_state.generator = WabaDataGenerator(seed=42)
    if "uploader" not in st.session_state:
        st.session_state.uploader = MinioUploader()
    if "batch_counter" not in st.session_state:
        st.session_state.batch_counter = 1

    gen: WabaDataGenerator = st.session_state.generator
    uploader: MinioUploader = st.session_state.uploader

    # Sidebar: Configurations
    st.sidebar.header("⚙️ Paramètres Généraux")

    # Countries selector
    selected_countries = st.sidebar.multiselect(
        "Pays cibles (ISO 3166-1 alpha-2)",
        options=COUNTRIES,
        default=COUNTRIES,
        help="CI, SN, ML, BF, GN, TG, BJ (Zone UEMOA / XOF) et GH (Ghana / GHS)"
    )

    # Entities selector
    selected_entities = st.sidebar.multiselect(
        "Lignes de métier / Entités",
        options=ENTITY_TYPES,
        default=ENTITY_TYPES
    )

    # Period selector
    st.sidebar.subheader("📅 Période Simulée")
    default_end = datetime.date.today()
    default_start = default_end - datetime.timedelta(days=90)
    col_d1, col_d2 = st.sidebar.columns(2)
    start_date = col_d1.date_input("Date début", value=default_start)
    end_date = col_d2.date_input("Date fin", value=default_end)

    start_dt = datetime.datetime.combine(start_date, datetime.time.min)
    end_dt = datetime.datetime.combine(end_date, datetime.time.max)

    # Main Tabs
    tab1, tab2, tab3 = st.tabs(["🚀 Génération des Transactions", "📚 Référentiels (Master Data)", "📊 Métadonnées & Stockage"])

    with tab1:
        st.markdown("### 1. Volumes par type de données transactionnelles")
        col1, col2, col3, col4 = st.columns(4)
        n_bank = col1.number_input("Transactions Bancaires", min_value=100, max_value=500_000, value=10_000, step=1_000)
        n_ins = col2.number_input("Opérations Assurance", min_value=100, max_value=200_000, value=5_000, step=500)
        n_mm = col3.number_input("Paiements Mobile Money", min_value=100, max_value=1_000_000, value=20_000, step=2_000)
        n_loan = col4.number_input("Remboursements Crédit", min_value=100, max_value=200_000, value=5_000, step=500)

        st.markdown("### 2. Mode d'exécution")
        mode = st.radio(
            "Sélectionner le mode d'ingestion :",
            ["One-time (Génération immédiate)", "Continue (Flux streaming régulier)"],
            horizontal=True
        )

        stream_interval = 15
        if mode == "Continue (Flux streaming régulier)":
            stream_interval = st.slider("Intervalle de rafraîchissement (secondes)", min_value=10, max_value=60, value=15)
            st.info(f"Le générateur produira des micro-lots de transactions toutes les {stream_interval} secondes.")

        # Trigger button
        if st.button("▶️ Démarrer la Génération & l'Envoi vers MinIO", type="primary"):
            if not selected_countries:
                st.error("Veuillez sélectionner au moins un pays.")
                return

            # Ensure master reference data is populated
            with st.spinner("Vérification des référentiels (Clients, Comptes, Agences)..."):
                gen.ensure_reference_data(scale_factor=0.05)

            date_str = datetime.date.today().strftime("%Y%m%d")
            seq = f"{st.session_state.batch_counter:02d}"
            st.session_state.batch_counter += 1

            progress_bar = st.progress(0)
            status_text = st.empty()

            total_steps = len(selected_countries) * 4
            curr_step = 0

            results_summary = []

            for cc in selected_countries:
                curr = CURRENCY_MAP[cc]

                # 1. Bank transactions
                if "BANK" in selected_entities:
                    status_text.text(f"[{cc}] Génération des transactions bancaires ({curr})...")
                    rows_bank = gen.generate_bank_transactions(cc, n_rows=int(n_bank // len(selected_countries)), start_date=start_dt, end_date=end_dt)
                    fname_bank = f"bank_txn_{cc}_{date_str}_{seq}.csv"
                    csv_data = gen.to_csv_string(rows_bank, list(rows_bank[0].keys()))
                    key = uploader.upload_raw_file(country_code=cc, entity_type="BANK", filename=fname_bank, data=csv_data)
                    results_summary.append({"Type": "BANK", "Pays": cc, "Fichier": fname_bank, "Lignes": len(rows_bank), "Devise": curr})
                curr_step += 1
                progress_bar.progress(curr_step / total_steps)

                # 2. Insurance
                if "INSURANCE" in selected_entities:
                    status_text.text(f"[{cc}] Génération des opérations d'assurance ({curr})...")
                    rows_ins = gen.generate_insurance_operations(cc, n_rows=int(n_ins // len(selected_countries)), start_date=start_dt, end_date=end_dt)
                    fname_ins = f"insurance_ops_{cc}_{date_str}_{seq}.csv"
                    csv_data = gen.to_csv_string(rows_ins, list(rows_ins[0].keys()))
                    key = uploader.upload_raw_file(country_code=cc, entity_type="INSURANCE", filename=fname_ins, data=csv_data)
                    results_summary.append({"Type": "INSURANCE", "Pays": cc, "Fichier": fname_ins, "Lignes": len(rows_ins), "Devise": curr})
                curr_step += 1
                progress_bar.progress(curr_step / total_steps)

                # 3. Mobile Money
                if "MOBILE_MONEY" in selected_entities and cc in ["CI", "SN", "BF", "GH"]:
                    status_text.text(f"[{cc}] Génération des paiements mobile money ({curr})...")
                    rows_mm = gen.generate_mobile_money_payments(cc, n_rows=int(n_mm // len(selected_countries)), start_date=start_dt, end_date=end_dt)
                    fname_mm = f"mobile_money_{cc}_{date_str}_{seq}.csv"
                    csv_data = gen.to_csv_string(rows_mm, list(rows_mm[0].keys()))
                    key = uploader.upload_raw_file(country_code=cc, entity_type="MOBILE_MONEY", filename=fname_mm, data=csv_data)
                    results_summary.append({"Type": "MOBILE_MONEY", "Pays": cc, "Fichier": fname_mm, "Lignes": len(rows_mm), "Devise": curr})
                curr_step += 1
                progress_bar.progress(curr_step / total_steps)

                # 4. Loan repayments
                if "BANK" in selected_entities or "MICROFINANCE" in selected_entities:
                    status_text.text(f"[{cc}] Génération des remboursements de crédit ({curr})...")
                    rows_loan = gen.generate_loan_repayments(cc, n_rows=int(n_loan // len(selected_countries)), start_date=start_dt, end_date=end_dt)
                    fname_loan = f"loan_repayments_{cc}_{date_str}_{seq}.csv"
                    csv_data = gen.to_csv_string(rows_loan, list(rows_loan[0].keys()))
                    key = uploader.upload_raw_file(country_code=cc, entity_type="BANK", filename=fname_loan, data=csv_data)
                    results_summary.append({"Type": "LOAN", "Pays": cc, "Fichier": fname_loan, "Lignes": len(rows_loan), "Devise": curr})
                curr_step += 1
                progress_bar.progress(curr_step / total_steps)

            status_text.text("✅ Ingestion terminée avec succès !")
            st.success(f"Tous les fichiers ont été déposés dans le bucket **raw-landing** (total : {len(results_summary)} fichiers).")
            st.dataframe(results_summary)

            if mode == "Continue (Flux streaming régulier)":
                st.info(f"Mode continu activé : prochain lot dans {stream_interval} secondes...")
                time.sleep(stream_interval)
                st.rerun()

    with tab2:
        st.markdown("### Génération des Tables Référentielles Partagées")
        st.info(
            "Ces tables constituent le socle de cohérence de l'ensemble du Lakehouse. "
            "Les transactions s'y réfèrent sans aucune clé orpheline."
        )

        col_r1, col_r2, col_r3 = st.columns(3)
        cust_target = col_r1.number_input("Clients (Cible finale 500k)", value=10_000, step=5_000)
        acc_target = col_r2.number_input("Comptes (Cible finale 800k)", value=20_000, step=5_000)
        branch_target = col_r3.number_input("Agences (200 agences)", value=200, step=20)

        if st.button("Générer & Déposer les Référentiels dans MinIO"):
            with st.spinner("Génération des tables maîtresses..."):
                gen.generate_customers(n_rows=int(cust_target))
                gen.generate_accounts(n_rows=int(acc_target))
                gen.generate_branches(n_rows=int(branch_target))
                ref_csvs = gen.get_reference_csvs()

                for fname, content in ref_csvs.items():
                    uploader.upload_reference_file(fname, content)

            st.success(f"Référentiels générés et déposés dans `raw-landing/references/` !")
            st.write(f"- Customers : {gen.store.total_customers:,} clients")
            st.write(f"- Accounts : {gen.store.total_accounts:,} comptes")
            st.write(f"- Branches : {gen.store.total_branches:,} agences")
            st.write(f"- Products : {len(gen.store.products):,} produits au catalogue")

    with tab3:
        st.markdown("### État du Stockage & Connecteur MinIO")
        st.write(f"- **Endpoint MinIO** : `{uploader.endpoint_url}`")
        st.write(f"- **Statut connexion S3** : {'🟢 Connecté' if uploader.is_connected else '🟡 Mode local (Simulation)'}")
        st.write(f"- **Buckets requis** : `raw-landing`, `lakehouse`, `archive`")
        if not uploader.is_connected:
            st.warning(
                "Le cluster MinIO distant n'est pas encore démarré. Les fichiers sont automatiquement "
                "organisés et sauvegardés sur le stockage local `./storage/minio/raw-landing/` "
                "conformément à l'arborescence cible."
            )


def run_cli_generation(args):
    """Génération par lot en ligne de commande pour les tests et la validation."""
    print("=" * 70)
    print("🚀 WABA Group - Générateur de données de test (Batch CLI)")
    print(f"Pays cibles : {args.countries} | Entités : {args.entities}")
    print("=" * 70)

    gen = WabaDataGenerator(seed=args.seed)
    uploader = MinioUploader(local_fallback_dir=args.output_dir)

    # 1. References
    print("\n[1/3] Generating reference datasets...")
    gen.generate_customers(n_rows=args.customers)
    gen.generate_accounts(n_rows=args.accounts)
    gen.generate_branches(n_rows=args.branches)

    for fname, csv_content in gen.get_reference_csvs().items():
        uploader.upload_reference_file(fname, csv_content)
        print(f"  -> Generated {fname}")

    # 2. Transactions
    print("\n[2/3] Generating transactions per country...")
    date_str = datetime.date.today().strftime("%Y%m%d")
    seq = "01"

    total_records = 0
    for cc in args.countries:
        curr = CURRENCY_MAP[cc]
        # Bank
        rows_bank = gen.generate_bank_transactions(cc, n_rows=args.bank_txns)
        fname = f"bank_txn_{cc}_{date_str}_{seq}.csv"
        uploader.upload_raw_file(cc, "BANK", fname, gen.to_csv_string(rows_bank, list(rows_bank[0].keys())))
        total_records += len(rows_bank)

        # Insurance
        rows_ins = gen.generate_insurance_operations(cc, n_rows=args.insurance_ops)
        fname = f"insurance_ops_{cc}_{date_str}_{seq}.csv"
        uploader.upload_raw_file(cc, "INSURANCE", fname, gen.to_csv_string(rows_ins, list(rows_ins[0].keys())))
        total_records += len(rows_ins)

        # Mobile Money
        if cc in ["CI", "SN", "BF", "GH"]:
            rows_mm = gen.generate_mobile_money_payments(cc, n_rows=args.mobile_money)
            fname = f"mobile_money_{cc}_{date_str}_{seq}.csv"
            uploader.upload_raw_file(cc, "MOBILE_MONEY", fname, gen.to_csv_string(rows_mm, list(rows_mm[0].keys())))
            total_records += len(rows_mm)

        # Loan repayments
        rows_loan = gen.generate_loan_repayments(cc, n_rows=args.loan_repayments)
        fname = f"loan_repayments_{cc}_{date_str}_{seq}.csv"
        uploader.upload_raw_file(cc, "BANK", fname, gen.to_csv_string(rows_loan, list(rows_loan[0].keys())))
        total_records += len(rows_loan)

    print("\n[3/3] Completed Generation Successfully!")
    print(f"Total Transactions Generated: {total_records:,}")
    print(f"Files landed in: raw-landing bucket (or {args.output_dir}/raw-landing)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WABA Data Generator")
    parser.add_argument("--cli", action="store_true", help="Run in headless CLI mode instead of Streamlit")
    parser.add_argument("--countries", nargs="+", default=COUNTRIES, help="Target countries")
    parser.add_argument("--entities", nargs="+", default=ENTITY_TYPES, help="Target business entities")
    parser.add_argument("--customers", type=int, default=5000, help="Customer count")
    parser.add_argument("--accounts", type=int, default=10000, help="Account count")
    parser.add_argument("--branches", type=int, default=200, help="Branch count")
    parser.add_argument("--bank-txns", type=int, default=1000, help="Bank transactions per country")
    parser.add_argument("--insurance-ops", type=int, default=500, help="Insurance operations per country")
    parser.add_argument("--mobile-money", type=int, default=2000, help="Mobile money transactions per country")
    parser.add_argument("--loan-repayments", type=int, default=500, help="Loan repayments per country")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output-dir", type=str, default="./storage/minio", help="Output directory")

    parsed_args, unknown = parser.parse_known_args()

    # If launched with `streamlit run app.py` or no --cli flag, run Streamlit UI
    if not parsed_args.cli and "streamlit" in sys.modules:
        run_streamlit_app()
    elif parsed_args.cli:
        run_cli_generation(parsed_args)
    else:
        # Default fallback to CLI if streamlit is not running the script
        try:
            import streamlit
            # If streamlit is imported and running in streamlit runtime
            if streamlit.runtime.exists():
                run_streamlit_app()
            else:
                run_cli_generation(parsed_args)
        except (ImportError, AttributeError):
            run_cli_generation(parsed_args)
