# Naming Convention:
# 1. BioTRACS_Merck_INV_Sampled_{YYYYMMDD_HHmmss}.csv (Multi-Project Upload)
# 2. For fields missing data, leave the field blank. Do not populate with “N/A,” “Not Available,” or “Not Provided.”
# 3. For numerical fields, do not populate null fields with a zero (0). Leave null.
# 4. Date fields format should be MM/DD/YYYY.
# 5. Text type fields have a maximum of 250 characters.
# 6. See Appendix 2. for value mappings

import json
import os
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import numpy as np
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from operators.ms_teams_webhook_hook import MSTeamsWebhookHook
from modules.sampledsphere_db.session import SessionLocal
from modules.sampledsphere_db.models import (
    Accessioning,
    Aliquot,
    QualityControl,
    StatusUpdates,
)
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from pytz import timezone
from sqlalchemy import func, or_
import shortuuid
import io

pd.set_option("max_columns", None)  # Showing only two columns
pd.set_option("max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

default_args = {
    "owner": "airflow-api",
    "email": ["alexander.bogdanowicz@infinity-biologix.com"],
    "email_on_failure": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

db = SessionLocal()

SHAREPOINT_URL = Variable.get("BIOSPHERE_SHAREPOINT_URL")
BASESITE = Variable.get("BIOSPHERE_SHAREPOINT_BASESITE")
USERNAME = Variable.get("BIOSPHERE_SHAREPOINT_USERNAME")
PASSWORD = Variable.get("BIOSPHERE_SHAREPOINT_PASSWORD")
SUBMITTER = Variable.get("LIMS_SUBMITTER")

MAPPING = {}

ACC_MAPPING = {
    "inventory_code": "Specimen ID",
    "analysis_type": "Analysis Type",
    "assay": "Assay",
    "draw_date": "Collection Date",
    "draw_time": "Collection Time",
    "created_on": "Created Date",
    "status": "Current Status",
    "site_name": "Destination Facility",
    "origination_facility": "Origination Facility",
    "destination_facility": "Destination Facility",
    "randomization_id": "Randomization ID",
    "screening_number": "Screening ID",
    "date_received": "Received Date",
    "site": "Site ID",
    # "comments" : "Specimen Comments",
    "comments": "Specimen Comments",
    "specimen_type": "Specimen Type",
    "study_name": "Study Number",
    "ruid": "Vendor Specimen ID",
    "family_id": "Visit",
    "container_type": "Container Type",
}

ALI_MAPPING = {
    "inventory_code": "Specimen ID",
    "parent_barcode": "Parent Specimen ID",
    "status": "Current Status",
    "ruid": "Vendor Specimen ID",
    "container_type": "Container Type",
    "aliquot_created_on": "Created Date",
    "specimen_type": "Specimen Type",
}

QC_MAPPING = {
    "inventory_code": "Specimen ID",
    "concentration": "Nucleic Acid Concentration",
    "vol_avg": "Nucleic Acid Volume",
    "yield": "Nucleic Acid Yield",
    "260_280": "Purity",
}

SU_MAPPING = {
    "inventory_code": "Specimen ID",
    "status": "Current Status",
    "site_name": "Destination Facility",
    "stored_date": "Stored Date",
    "shipped_date": "Shipped Date",
    "disposed_date": "Terminal Date",
}

STATUS_MAP = {
    "Stored": "In Inventory",
    "Shipped": "In Transit",
    "Disposed": "Exhausted",
    "Released": "In Inventory",
    "Sentout": "In Transit",
    "Failed": "In Inventory",
    "Distribution": "In Inventory",
    "Discarded": "Exhausted",
    "Quarantine": "In Inventory",
    "ToBeShipped": "In Inventory",
    "InProcess": "In Inventory",
}

FACILITY_MAP = {
    "MSD-ATC Pharma": "Site",
    "LabCorp Drug Development-Europe": "Lab Corp",
    "LabCorp Drug Development-USA": "Lab Corp",
    "MSD- SGS Belgium": "site",
    "LabCorp Drug Development - Asia": "Lab Corp",
    "MSD - UZ Leuven - Campus Gasthuisberg": "Site",
    "MSD-UZGent-Drug Research Unit Ghent": "Site",
    "MSD-CCP Leuven": "Site",
    "MSD - Belgium": "site",
    "MSD-Collaborative NeuroScience Network": "Site",
    "MSD-Clinical Pharmacology of Miami": "Site",
    "MSD- UZ Gent-Drug": "Site",
    "MSD-MD Clinical": "Site",
    "MSD-Clinical Research Hospital Tokyo": "Site",
    "MSD-Medical Corp Heishinkai OPHAC Hos": "Site",
    "MSD-Yale University": "Site",
    "MSD-Thomas Jefferson University": "Site",
    "MSD - Profil Institute for Clinical Research": "Site",
    "MSD-SGS Clinical Pharmacology Unit": "Site",
    "MSD-Research Centers of America LLC": "Site",
    "Celerion-Nebraska": "Celerion",
    "UZ Leuven - Campus Gasthuisber": "Site",
    "MSD - Japan": "MSD - Japan",
    "MSD-Collaborative Neuroscience Research LLC": "Site",
    "MSD-P-One Clinic": "Site",
    "MSD- UZ Ghent": "Site",
    "MSD-UZ GENT": "Site",
    "MSD-SGS Belgium": "Site",
    "MSD-Hassman Research Institute Marlton Site": "Site",
    "MSD-Hassman Research Insitute": "Site",
    "MSD-Genesis Clinical Research": "Site",
    "MSD-Jasz Nagykun Szolnok Megyei Hetenyi": "Site",
    "MSD-BioKinetic Clinical Applications": "Site",
    "Coriell Institute Med Research": "Site",
    "MSD-QPS Miami Reaearch Associates": "QPS",
    "MSD-AMR-NOCCR": "Site",
    "Celerion-Arizona": "Celerion",
    "MSD-PRA Health Sciences Research Martini": "ICON",
    "MSD-Advanced Pharma CR, LLC": "Site",
    "Drug Research Unit Gent": "Site",
    "SGS Life Science Services": "Site",
    "Worldwide Clinical Trials": "Site",
    "MSD - Hospital Brotonneau": "Site",
    "Hiroshima Allergy & Respiratory Clinic": "Site",
    "Takahashi Clinic": "Site",
    "MSD-hVIVO Queen Mary Centre": "hVIVO Services Limited",
    "MSD-Republican Clinical Hospital of Moldova": "Site",
    "MSD-Chaim Sheba Medical Center": "Site",
    "MSD-Hospital Universitario Central de Asturias": "Site",
    "MSD- Hospital Universitario de Asturias": "Site",
    "MSD-H.U Vall de Hebron": "Site",
    "MSD": "site",
    "MSD - Spain": "MSD - Spain",
    "MSD-Hospital Puerta de Hierro": "Site",
    "MSD-Hospital Universitario Insular DE G": "Site",
    "Koukokukai Ebisu Medical Clinic": "Site",
    "Q2 Solutions-Clinica De Neumologia": "Q2",
    'Alergologiczno-Internis "All-Med"': "Site",
    "MSD-New Orleans Center for Clinical Research": "Site",
    "MSD-Vince and Associates Clin Research Associates": "Site",
    "MSD-Liege- A.T.C. S.a.": "Site",
    "Brooks Life Sciences": "Azenta",
    "Koizumi Pulmonology & Internal Medicine Clinic": "Site",
    "MSD-Houston,TX": "site",
    "Keikokai Medical": "Site",
    "MSD Pulmonary Associates": "Site",
    "Uni. Szpital Kliniczny NR 1": "Site",
    "Nzoz Centrum Badan Klinicznych": "Site",
    "Sekino Hospital": "Site",
    "Q2 Solutions, Karl Bremer Hospital": "Q2",
    "Centrum Medycyny Oddechowej": "Site",
    "Q2 Solutions, Synexus Helderberg Clinical Trials": "Q2",
    "Kanazawa Municipal Hospital": "Site",
    "Shibasaki Internal Medicine and Pediatrics Clinic": "Site",
    "Q2 Solutions, Engelbrecht Research": "Q2",
    "Medical Corporation Ocrom Clinic": "Site",
    "Lung Clinical Research Unit": "Site",
    "Kawarada Clinic": "Site",
    "Doujin Memorial Medical Foundation": "Site",
    "Tosei General Hospital": "Site",
    "Idaimae Minamiyojo Int Clinic": "Site",
    "Profil Institute for Clinical Research, Inc.": "Site",
    "MSD - London": "site",
    "Karl Bremer Hospital - Tiervlei Trial Centre": "Site",
    "University Teknologi Mara": "Site",
    "Hiramatsu Internal & Respiratory Medicine": "Site",
    "Q2 Solutions - SHOP 6, Freeway Plaza": "Q2",
    "Nakatani Hospital": "Site",
    "Kikuchi Clinic": "Site",
    "Yamazaki Internal Medicine Clinic": "Site",
    "Q2 Solutions - Dr. Servet Menendez": "Q2",
    "Hospital Taiping": "Site",
    "Osaki Internal and Respiratory Clinic": "Site",
    "Q2 Solutions - Dr. Gerardo Martinez": "Q2",
    "Tokyo Center Clinic": "Site",
    "Medical Corp Tohda Clinic": "Site",
    "Yokohama City Minato Red Cross Hospital": "Site",
    "Funaijibiinkoka Clinic": "Site",
    "Worldwide Clinical Trials Early Phase Ser LLC": "Site",
    "MSD - OLV Aalst": "Site",
    "MSD-Prism  Research": "Site",
    "MSD - Boston": "MRL - Boston",
    "Otogenetics": "Site",
    "Northside Hospital": "Site",
    "BGI CHOP": "BGI",
    "Showa University Hospital": "Site",
    "Pharmaron CPC": "Site",
    "IPS": "Flagship Biosciences",
    "MSD - Berlin": "Site",
    "MSD-Gent": "Site",
    "Universitair Ziekenhuis Gent": "Site",
    "Novum Pharma Research": "Site",
    "MSD- Centre Hospitalier Universitaire de Liege": "site",
    "MERCK-Maccabi Healthcare Services": "Site",
    "Kouwakai Kouwa Medical Clinic": "Site",
    "Kono Medical Clinic": "Site",
    "Kaiseikai Kita Shin Yokohama Internal Medicine Clinic": "Site",
    "Kinki University Hospital": "Site",
    "National Mie Hospital": "Site",
    "Q2 Solutions - Dr. Guerra Mejia": "Q2",
    "Jalan Taming Sari": "Site",
    "Inobefunai Clinic": "Site",
    "MSD - Orlando Clinic Research Center": "Site",
    "Altasciences": "Site",
    "Merck Sharp & Dohme, Corp. Oita University Hospital": "Site",
    "MSD-Oita": "Site",
    "MSD-QPS": "QPS",
    "MSD-Clinilabs, INC.": "Site",
    "MSD - Department of Medica - Calvary Mater Newcastle": "Site",
    "MSD - Clinilabs Inc.": "Site",
    "MSD-Charite Research(GmbH)": "Site",
    "MSD-Hospital General Universitario 12 de Octubre": "Site",
    "MSD - Altasciences Kansas City": "Site",
    "MSD - Singapore": "site",
    "MSD-Univeritair Ziekenhuis Gent": "Site",
    "MSD-Charite Research Organisation GmbH": "Site",
    "Q2 Solutions - Inglewood": "Q2",
    "MSD-Charity-Universitaetsmedizin": "Site",
    "MSD-ProSciento Inc.": "Site",
    "MSD-AMR-Lexington": "Site",
    "MSD-Medstar Good Samaritan Hospital": "Site",
    "MSD-Massachusetts General Hospital": "Site",
    "MSD - ProSciento Inc.": "Site",
    "MSD-PRA Health Sciences": "ICON",
    "MSD-Beth Israel Deaconess Medical Center": "Site",
    "MSD - Karolinska University Hospital": "Site",
    "MSD - Israel": "Site",
    "MSD- Orszagos Koranyi TBC es Pulmonologiai Intezet": "Site",
    "MSD- University of Colorado": "Site",
    "Monogram Biosciences": "Monogram",
    "MSD-Oncology Institute": "Site",
    "MSD-Tibor Csoszi": "Site",
    "MSD-Hospital General Universitario": "Site",
    "MSD-Petz Aladar Megyei Oktato Korhaz": "Site",
    "MSD-Weinberg Cancer Institute": "Site",
    "MSD-CISSSMC-Hospital Charles-LeMoyne": "Site",
    "MSD-Cancer Centre of Ontario": "Site",
    "MSD-Global Central Labs": "Site",
    "MSD-San Antonio": "Site",
    "Covance CRU": "Lab Corp",
    "MSD-Woodland Research Northwest LLC": "Site",
    "MSD-Centre for the Evaluation of Vaccination": "Site",
    "Quest Clinical Research": "Q2",
    "MSD-CHRU Lille": "Site",
    "Celerion-Nebraska 2": "Celerion",
    "MSD - Beth Israel Deaconess Medical Center": "Site",
    "MSD-California Clinical Trails Medical Group": "Site",
    "MSD-Velocity Clinical Research": "Site",
    "MSD-Arensia Exploratory Medicine-Clinical Nephro": "Site",
    "MSD-Texas Liver Institute": "Site",
    "MSD-MD Clinical Miami": "Site",
    "Merck Sharp & Dohme, CORP.": "Merck",
    "Shinjo Naika Clinic": "Site",
    "MSD-Dana Farber Cancer Institute": "Site",
    "Merck Sharp & Dohme- Hungary": "?",
    "MSD-AOUS, Immunoterapia Oncologia": "Site",
    "MSD-Kingston General Hospital": "Site",
    "MSD-Celerion": "Celerion",
    "Interpace Pharma Solutions": "Flagship Biosciences?",
    "Research Centurion": "Site",
    "MSD-QPS-MRA, LLC-Early Phase": "QPS",
    "MSD-Maccabi": "Site",
    "MSD-SCRI": "Site",
}

REQUIRED = [
    "Analysis Type",
    "Created Date",
    "Current Status",
    "Origination Facility",
    "Received Date",
    "Specimen ID",
    "Specimen Type",
    "Study Number",
    "Vendor",
]

ALL_COLUMNS = [
    "Analysis Type",
    "Assay",
    "Biopsy Accession ID",
    "Biopsy Anatomic Location",
    "Biopsy Collection Method",
    "Collection Date",
    "Created Date",
    "Current Status",
    "Destination Facility",
    "Fixation Method",
    "Lesion Type",
    "Nucleic Acid Concentration",
    "Nucleic Acid Volume",
    "Nucleic Acid Yield",
    "Origination Facility",
    "Parent Specimen ID",
    "Pre/Post Treatment",
    "Randomization ID",
    "Received Date",
    "Screening ID",
    "Shipped Date",
    "Site ID",
    "Slide Thickness",
    "Slides Sectioned Date",
    "Specimen Comments",
    "Specimen Fixation Date",
    "Specimen ID",
    "Specimen Tissue Category",
    "Specimen Type",
    "Study Number",
    "Terminal Date",
    "Terminal Date_su",
    "Vendor",
    "Vendor Specimen ID",
    "Visit",
    "Diagnosis Confirmed",
    "Biopsy Lesion Injection Status",
    "Time from Tissue Excision to Immersion in Fixative",
    "Fixation Time",
    "Institutional Block or Slide ID",
    "Time Specimen Placed in Fixative",
    "Number of Slides Submitted",
    "Collection Time",
    "Purity",
    "Type of Biopsy Sample Taken",
    "Container Type",
]

ANALYSIS_MAPPING = {
    "RNA analysis": "RNA Analysis",
    "Genetic Anaylsis": "Genetic Analysis",
    "Correlative": "Correlative",
    "Targeted Genotyping": "Targeted Genotyping",
    "Genetic analysis (Buccal Swab)": "Genetic Analysis (Buccal Swab)",
    "ctDNA": "ctDNA",
    "T-cell repertoire (TCR)": "T-cell Repertoire (TCR)",
    "T-cell Repertoire (TCR)": "T-cell Repertoire (TCR)",
    "T-cell Rpertoire (TCR)": "T-cell Repertoire (TCR)",
    "Legacy": "Legacy",
    "Genetic analysis": "Genetic Analysis",
    "Genetic Analysis (Buccal Swab)": "Genetic Analysis (Buccal Swab)",
    "RNA Analysis": "RNA Analysis",
    "Blood For RNA Analysis": "RNA Analysis",
    "Targeted genotyping": "Targeted Genotyping",
    "T-Cell Repertoire (TCR)": "T-cell Repertoire (TCR)",
    "CYP2C9 Genotyping": "CYP2C9 Genotyping",
    "Future Biomedical Research": "Future Biomedical Research",
    "Genetic Analysis": "Genetic Analysis",
    "Correlative ": "Correlative",
    "Generic Analysis": "Genetic Analysis",
    "Genetic Analysis ": "Genetic Analysis",
    "Genetoc Analysis": "Genetic Analysis",
    "RNA Analysis ": "RNA Analysis",
}

SOURCE_MAPPING = {
    "10 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "10 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "10.0 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "10.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "10.0mL Paxgene DNA": "Whole Blood EDTA - DNA",
    "2mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "BC": "Buffy Coat",
    "BUC": "Buccal Swab",
    "d": "Whole Blood PAXgene - DNA",
    "DNA-BUC": "DNA (Buccal Swab)",
    "DNA-EP": "DNA",
    "DNA-Saliva": "DNA (Saliva)",
    "DNA-WB": "DNA",
    "EP-DNA	": "DNA",
    "PL": "Plasma",
    "RNA-EP": "RNA",
    "RNA-WB": "RNA",
    "S": "Saliva",
    "Whole Blood": "Whole Blood EDTA - DNA",
    "Whole Blood PAXgene - DNA": "Whole Blood PAXgene - DNA",
    "TISDGHRT": "Tissue",
    "EPPL": "Plasma",
    "DNA-EPCACL": "DNA",
    "DNA-EPCP": "DNA",
    "DNA-EPMCACL": "DNA",
    "DNA-EPPL": "DNA",
    "DNA-LCL": "DNA",
    "EPCACL": "DNA",
    "EPCL": "DNA",
    "EPMCACL": "DNA",
    "LCL": "DNA",
    "TISBST": "Tissue",
    "TISHMTH": "Tissue",
    "TISLUNG": "Tissue",
    "TISOVY": "Tissue",
    "TISSTM": "Tissue",
}

SPECIMEN_MAPPING = {
    "10 mL EDTA": "Whole Blood EDTA - DNA",
    "10 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "10 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "10 mL PAXgne RNA": "Whole Blood PAXgene - RNA",
    "10 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "10.0 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "10.0 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "10.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "10.0 mL Streck Tan BCT": "Blood",
    "10.0 Paxgene RNA": "Whole Blood PAXgene - RNA",
    "10.0 Purple Top Tube": "Whole Blood EDTA - DNA",
    "10.0mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "10.0mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "10.0mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "10mL PAXgene DNA tube": "Whole Blood PAXgene - DNA",
    "10mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "11 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "11 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "11 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "12 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "12 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "12 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "13 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "13 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "13 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "14 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "14 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "14 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "15 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "15 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "15 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "16 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "16 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "16 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "17 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "17 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "17 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "18 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "18 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "18 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "19 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "19 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "19 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "2.0 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "2.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "2.0mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "2.5 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "20 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "20 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "20 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "21 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "21 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "21 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "22 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "22 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "22 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "23 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "23 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "23 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "24 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "24 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "24 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "25 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "25 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "25 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "26 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "26 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "26 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "27 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "27 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "27 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "28 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "28 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "28 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "29 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "29 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "29 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "2mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "3.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "30 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "30 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "30 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "31 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "31 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "32 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "32 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "33 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "33 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "34 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "34 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "35 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "35 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "36 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "36 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "37 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "37 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "38 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "38 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "39 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "39 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "4.0 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "4.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "4.0 mL Purple Top Tubes": "Whole Blood EDTA - DNA",
    "4.0 mL Purple TopTube": "Whole Blood EDTA - DNA",
    "40 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "40 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "41 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "41 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "42 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "42 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "43 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "43 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "44 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "44 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "45 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "45 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "46 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "46 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "47 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "47 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "48 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "48 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "49 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "49 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "50 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "50 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "51 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "51 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "52 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "52 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "53 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "53 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "54 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "54 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "55 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "55 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "56 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "56 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "57 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "57 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "58 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "59 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "6.0 mL EDTA DNA": "Whole Blood EDTA - DNA",
    "6.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "6.0mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "60 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "61 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "62 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "63 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "64 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "65 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "10 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "10 mL PAXgene DNA": "Whole Blood PAXgene - DNA",
    "10 mL Paxgene RNA": "Whole Blood PAXgene - RNA",
    "10 mL PAXgene RNA": "Whole Blood PAXgene - RNA",
    "10 mL Purple Top Tube DNA": "Whole Blood EDTA - DNA",
    "10.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "10.0 mL Streck Tan BCT": "Whole Blood - Streck Plasma",
    "2.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "3.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "3.0 mL Purple Top Tube DNA": "Whole Blood EDTA - DNA",
    "4 mL Paxgene DNA": "Whole Blood PAXgene - DNA",
    "4.0 mL Purple Top Tube": "Whole Blood EDTA - DNA",
    "4.0 mL Purple Top Tube DNA": "Whole Blood EDTA - DNA",
    "6.0 mL Purple Top Tube DNA": "Whole Blood EDTA - DNA",
    "BloodSpotCard": "Blood Spot Card",
    "Micronic 1.4": "use Source Matcode mapping",
    "Sarstedt 2.0": "request Specimen Type from Merck",
    "Sarstedt 5.0": "request Specimen Type from Merck",
    "EPPL": "Plasma",
    "DNA-EPCACL": "DNA",
    "DNA-EPCP": "DNA",
    "DNA-EPMCACL": "DNA",
    "DNA-EPPL": "DNA",
    "DNA-LCL": "DNA",
    "EPCACL": "DNA",
    "EPCL": "DNA",
    "EPMCACL": "DNA",
    "LCL": "DNA",
    "TISBST": "Tissue",
    "TISHMTH": "Tissue",
    "TISLUNG": "Tissue",
    "TISOVY": "Tissue",
    "TISSTM": "Tissue",
}

SPECIMEN_MAPPING = {
    **SPECIMEN_MAPPING,
    **{key.lower(): value for key, value in SPECIMEN_MAPPING.items()},
}

ANALYSIS_TYPES = set(
    [
        "Virology",
        "Viremia",
        "Viral Shedding",
        "Viral Resistance",
        "UGT2B17 Genotyping",
        "TOR-BMx",
        "T-cell Repertoire (TCR)",
        "T-cell Evaluation",
        "Targeted Genotyping",
        "TARC",
        "sILT3",
        "RT-PCR (Zika)",
        "RT-PCR (Chikungunya)",
        "RT-PCR",
        "RSV A and B Ab Titers",
        "RSV (SNAb)",
        "RNA Analysis",
        "Receptor Occupancy",
        "Pneumococcal Serotype Eval",
        "Pneumococcal Poly Shed Assess",
        "Pneumococcal Immunogenicity Assay",
        "Pneumococcal Colonization Assess",
        "PK-REL IMI",
        "PK-REL",
        "PK-MK8591",
        "PK-MK8558",
        "PK-MK3402",
        "PK-M9",
        "PK-LNG",
        "PK-Fluoride",
        "PK-FEP",
        "PK-EE",
        "PK - ISL",
        "PK - IRL",
        "PK - DOR",
        "PK",
        "PD-L1",
        "PBMC",
        "MSI",
        "mRNA Profiling",
        "miRNA",
        "Metagenomics",
        "Metabolomics",
        "Metabolome",
        "MDSC Assay",
        "Leukapheresis",
        "Legacy",
        "Influenza Immunogenicity Assay",
        "Immunophenotyping",
        "Immunogenicity Assay (MOPA)",
        "Immunogenicity Assay (MOPA 2)",
        "Immunogenicity Assay (MOPA 1)",
        "Immunogenicity Assay (Hep B)",
        "Immunogenicity Assay (ECL)",
        "Immunogenicity Assay",
        "Immunogenicity (Spike IgG)",
        "Immunogenicity (Residual)",
        "Immunogenicity (Nab)",
        "Immune Profiling (Heparin)",
        "Immune Profiling",
        "IL-10",
        "HPV Type",
        "HLA",
        "HIV-1 RNA",
        "HIV-1 Drug Resistance",
        "HCV RNA",
        "HCV Genotyping",
        "Genetic Analysis (Buccal Swab)",
        "Genetic Analysis",
        "Future Biomedical Research (Saliva)",
        "Future Biomedical Research (Buccal Swab)",
        "Future Biomedical Research",
        "Fresh Tumor Tissue Collection",
        "EBV DNA",
        "DNA Seq",
        "Cytokines",
        "CYP2C9 Genotyping",
        "ctDNA EGFR/ALK",
        "ctDNA (specific)",
        "ctDNA",
        "CTC - Exploratory",
        "CTC",
        "Correlative",
        "CMV - DNA",
        "Citrate Coagulation",
        "C. Difficile",
        "Bone Marrow (RNA)",
        "Bone Marrow (DNA)",
        "Bone Marrow",
        "Biomarkers",
        "B-cell Immortalization",
        "B Cell Isolation",
        "Avidity",
        "AT Test",
        "AR-V7",
        "Archival or Newly Obtained Collection",
        "APOE",
        "Anti-RSV (IgA)",
        "Anti-HPV Antibody Testing",
        "Antibodies",
        "Alzheimer’s Disease Diagnosis",
        "ADA",
        "2C19 Genotyping",
        "ADA",
        "Antibodies",
        "Archival or Newly Obtained Collection",
        "AR-V7",
        "TBD",
        "Biomarkers",
        "Bone Marrow",
        "Correlative",
        "ctDNA",
        "CYP2C9 Genotyping",
        "Cytokines",
        "Future Biomedical Research",
        "Genetic Analysis",
        "Genetic Analysis (Buccal Swab)",
        "HCV RNA",
        "HIV-1 Drug Resistance",
        "HIV-1 RNA",
        "Immunogenicity (Nab)",
        "Immunogenicity (Spike IgG)",
        "Immunogenicity Assay",
        "Legacy",
        "Metabolome",
        "Metabolomics",
        "Metagenomics",
        "mRNA Profiling",
        "MSI",
        "PBMC",
        "PK",
        "PK - DOR",
        "PK - ISL",
        "RNA Analysis",
        "RSV A and B Ab Titers",
        "Targeted Genotyping",
        "T-cell Repertoire (TCR)",
        "UGT2B17 Genotyping",
        "Viral Shedding",
        "Viremia",
        "NULL",
    ]
)

SPECIMEN_TYPES = set(
    [
        "Whole Blood PAXgene - RNA",
        "Whole Blood PAXgene - DNA",
        "Whole Blood EDTA - DNA",
        "Whole Blood EDTA - Cell Pellet",
        "Whole Blood (EDTA)",
        "Urine",
        "Tissue",
        "Stool",
        "Sputum",
        "Solution",
        "Slide Scroll",
        "Slide Holder",
        "Slide",
        "Serum",
        "Saliva",
        "RNA",
        "Plasma Peptide",
        "Plasma",
        "PBMC",
        "Oral Swab",
        "Nasal Wash",
        "Nasal Swab",
        "Middle Ear Fluid",
        "DNA (Urine)",
        "DNA (Saliva)",
        "DNA (EDTA)",
        "DNA (Buccal Swab)",
        "DNA",
        "Cerebrospinal Fluid",
        "cDNA",
        "Buffy Coat",
        "Buccal Swab",
        "Bone Marrow Biopsy",
        "Bone Marrow Aspiration (Heparin)",
        "Bone Marrow Aspiration (EDTA)",
        "Bone Marrow Aspiration",
        "Blood",
        "Block",
        "Biopsy",
        "Plasma",
        "Whole Blood PAXgene - RNA",
        "Whole Blood PAXgene - DNA",
        "TBD",
        "Plasma",
        "Serum",
        "Biopsy",
        "Whole Blood (EDTA)",
        "Block",
        "Blood",
        "Bone Marrow Aspiration",
        "Buccal Swab",
        "Cerebrospinal Fluid",
        "DNA",
        "RNA",
        "Whole Blood EDTA - DNA",
        "DNA (Buccal Swab)",
        "Slide",
        "NULL",
        "Nasal Swab",
        "PBMC",
        "Saliva",
        "Stool",
        "Tissue",
        "Urine",
    ]
)

# P3_STUDY = [
#    "MK-0000-386", "MK-0000-387", "MK-0431-838", "MK-0431-845", "MK-0431-848", "MK-0822-018", "MK-1029-001", "MK-1029-003", "MK-1029-004", "MK-1029-005", "MK-1029-006", "MK-1029-008", "MK-1029-011", "MK-1029-012", "MK-1029-015", "MK-1029-017", "MK-1075-001", "MK-1092-001", "MK-1439-007", "MK-1439-018", "MK-1439-042", "MK-1439-044", "MK-1439-045", "MK-1439-046", "MK-1439-048","MK-1439-049", "MK-1439-050", "MK-1439-051", "MK-1439-052", "MK-1439-053", "MK-1439A-054", "MK-1942-001", "MK-1942-003", "MK-1986-004", "MK-2075-001", "MK-2640-001", "MK-2888-001", "MK-3682-014", "MK-3682-023", "MK-3682-026", "MK-3682-029", "MK-3682-035", "MK-3682A-018", "MK-3682A-019", "MK-3682B-025", "MK-3682B-030", "MK-3682B-031", "MK-3682B-032", "MK-3682C-028", "MK-3682C-039", "MK-3682C-044","MK-3682C-045", "MK-3866-001", "MK-3866-002", "MK-3866-005", "MK-3866-006", "MK-3866-008", "MK-4250-001", "MK-4250-005", "MK-4334-001", "MK-4710-001", "MK-4710-002", "MK-4710-003", "MK-5160-001", "MK-5172-052", "MK-5172-058", "MK-5172-062", "MK-5172-068", "MK-5172-074", "MK-5172-078", "MK-5172-080", "MK-5172-081", "MK-5172-082", "MK-5348-015", "MK-5348-046", "MK-5475-001", "MK-5592-097", "MK-6158-001", "MK-6240-001", "MK-6884-001","MK-6884-002", "MK-7264-024", "MK-7264-025", "MK-7264-026", "MK-7264-028", "MK-7264-032", "MK-7625A-013", "MK-7655A-019", "MK-7680-001", "MK-7680-003", "MK-8056-001", "MK-8189-003", "MK-8189-006", "MK-8228-005", "MK-8228-023", "MK-8228-025", "MK-8228-029", "MK-8228-031", "MK-8228-032", "MK-8228-033", "MK-8228-034", "MK-8228-035", "MK-8228-036", "MK-8228-037", "MK-8237-001", "MK-8246-075", "MK-8342B-069", "MK-8408-004", "MK-8408-010", "MK-8504-001","MK-8504-002", "MK-8504-003", "MK-8507-001", "MK-8507-002", "MK-8507-005", "MK-8521-004", "MK-8583-001", "MK-8591-002", "MK-8591-003", "MK-8591-005", "MK-8591-006", "MK-8591-007", "MK-8591-009", "MK-8591-010", "MK-8591-011", "MK-8616-038", "MK-8616-101", "MK-8666-001", "MK-8666-002", "MK-8666-003", "MK-8666-004", "MK-8666-005", "MK-8666-006", "MK-8666-008", "MK-8719-001", "MK-8719-002", "MK-8719-003", "MK-8723-001", "MK-8768-001", "MK-8931-016","MK-8931-030", "MK-8931-032", "P04103", "V501-200"
# ]

P3_STUDY = [
    "MK0000386",
    "MK0000387",
    "MK0431838",
    "MK0431845",
    "MK0431848",
    "MK0822018",
    "MK1029001",
    "MK1029003",
    "MK1029004",
    "MK1029005",
    "MK1029006",
    "MK1029008",
    "MK1029011",
    "MK1029012",
    "MK1029015",
    "MK1029017",
    "MK1075001",
    "MK1092001",
    "MK1439007",
    "MK1439018",
    "MK1439042",
    "MK1439044",
    "MK1439045",
    "MK1439046",
    "MK1439048",
    "MK1439049",
    "MK1439050",
    "MK1439051",
    "MK1439052",
    "MK1439053",
    "MK1439A054",
    "MK1942001",
    "MK1942003",
    "MK1986004",
    "MK2075001",
    "MK2640001",
    "MK2888001",
    "MK3682014",
    "MK3682023",
    "MK3682026",
    "MK3682029",
    "MK3682035",
    "MK3682A018",
    "MK3682A019",
    "MK3682B025",
    "MK3682B030",
    "MK3682B031",
    "MK3682B032",
    "MK3682C028",
    "MK3682C039",
    "MK3682C044",
    "MK3682C045",
    "MK3866001",
    "MK3866002",
    "MK3866005",
    "MK3866006",
    "MK3866008",
    "MK4250001",
    "MK4250005",
    "MK4334001",
    "MK4710001",
    "MK4710002",
    "MK4710003",
    "MK5160001",
    "MK5172052",
    "MK5172058",
    "MK5172062",
    "MK5172068",
    "MK5172074",
    "MK5172078",
    "MK5172080",
    "MK5172081",
    "MK5172082",
    "MK5348015",
    "MK5348046",
    "MK5475001",
    "MK5592097",
    "MK6158001",
    "MK6240001",
    "MK6884001",
    "MK6884002",
    "MK7264024",
    "MK7264025",
    "MK7264026",
    "MK7264028",
    "MK7264032",
    "MK7625A013",
    "MK7655A019",
    "MK7680001",
    "MK7680003",
    "MK8056001",
    "MK8189003",
    "MK8189006",
    "MK8228005",
    "MK8228023",
    "MK8228025",
    "MK8228029",
    "MK8228031",
    "MK8228032",
    "MK8228033",
    "MK8228034",
    "MK8228035",
    "MK8228036",
    "MK8228037",
    "MK8237001",
    "MK8246075",
    "MK8342B069",
    "MK8408004",
    "MK8408010",
    "MK8504001",
    "MK8504002",
    "MK8504003",
    "MK8507001",
    "MK8507002",
    "MK8507005",
    "MK8521004",
    "MK8583001",
    "MK8591002",
    "MK8591003",
    "MK8591005",
    "MK8591006",
    "MK8591007",
    "MK8591009",
    "MK8591010",
    "MK8591011",
    "MK8616038",
    "MK8616101",
    "MK8666001",
    "MK8666002",
    "MK8666003",
    "MK8666004",
    "MK8666005",
    "MK8666006",
    "MK8666008",
    "MK8719001",
    "MK8719002",
    "MK8719003",
    "MK8723001",
    "MK8768001",
    "MK8931016",
    "MK8931030",
    "MK8931032",
    "P04103",
    "V501200",
]

# Questions that need to be answered:
# 1. Analysis Type NEEDS to be derived from another field
# 2. Specimen Type NEEDS to be derived from another field
# 3. Origination Facility Seems like it's missing from the BI Data? Is it there + did I miss it?
# 4. Origination Facilities are they standardized + recorded at accessioning?
# 5. Concentration should always be ng / ul in LIMS?
# 6. There seem to be null values for volume_unit in LIMS along with vol_avg - How Should this be interpreted?


@dag(
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule_interval=None,
    tags=["sampledsphere"],
)
def sphere_merck_data_feed_new():
    # Step 1. Create the Mapping from Aliquot to

    def send_data(file_name, data):
        print("Sending Data to Biotracs")
        s3_bucket = "biosphere-merck-sftp"
        s3 = S3Hook(aws_conn_id="lims_biosphere_s3_conn")
        key = f"biotracs/{file_name}"
        check = s3.check_for_key(key=key, bucket_name=s3_bucket)
        if check:
            raise AirflowException(f"S3 Bucket already contains content: {file_name}")
        try:
            s3.load_string(data.to_csv(index=False), key=key, bucket_name=s3_bucket)
        except:
            raise AirflowException(f"Trouble Transfering File to Biotracs: {file_name}")

    def run_acc_validation(accessioning):
        accessioning["origination_facility"] = (
            accessioning["origination_facility"]
            .replace(FACILITY_MAP)
            .replace([np.nan], [None])
        )
        accessioning["analysis_type"] = accessioning["analysis_type"].replace(
            ANALYSIS_MAPPING
        )
        accessioning["specimen_type"] = accessioning["source"].replace(SOURCE_MAPPING)
        accessioning = accessioning[
            ~(
                (accessioning["container_type"].astype(str) == "Micronic 1.4")
                & (accessioning["source"] == "WB")
            )
        ]
        # accessioning = accessioning[~((accessioning["container_type"] == "Micronic 1.4") & (accessioning["source"] == "WB"))]
        accessioning["specimen_type"] = accessioning.apply(
            lambda x: SPECIMEN_MAPPING[x["container_type"].lower()]
            if x["source"] == "WB"
            else x["specimen_type"],
            axis=1,
        )
        # miss_at = accessioning[~accessioning["analysis_type"].isin(ANALYSIS_TYPES)]
        # if miss_at.shape[0]:
        #     raise AirflowException(f"Incorrect Analysis Type (Accessioning): {' ,'.join(miss_at['inventory_code'])}")
        # miss_st = accessioning[~accessioning["specimen_type"].isin(SPECIMEN_TYPES)]
        # if miss_st.shape[0]:
        #     raise AirflowException(f"Incorrect Specimen Type (Accessioning): {' ,'.join(miss_st['inventory_code'])}")
        # miss_of = accessioning[~accessioning["origination_facility"].isin(FACILITY_MAP.keys())]
        # if miss_of.shape[0]:
        #     raise AirflowException(f"Incorrect Origination Facility (Accessioning): {' ,'.join(miss_of['inventory_code'])}")
        miss_stat = accessioning[~accessioning["status"].isin(STATUS_MAP.keys())]
        if miss_stat.shape[0]:
            raise AirflowException(
                f"Incorrect Status (Accessioning): {' ,'.join(miss_stat['inventory_code'])}"
            )
        accessioning["randomization_id"] = accessioning.apply(
            lambda x: str(x["randomization_id"]).zfill(6)
            if x["randomization_id"]
            else None,
            axis=1,
        )
        accessioning["screening_number"] = accessioning.apply(
            lambda x: str(x["screening_number"]).zfill(9)
            if x["screening_number"]
            else None,
            axis=1,
        )

        def check_site(inventory_code, site):
            numeric_site = pd.to_numeric(site, errors="coerce")
            try:
                if not np.isnan(numeric_site):
                    return str(int(pd.to_numeric(site))).zfill(4)
            except:
                print("normal site", site)
                print("numeric site", numeric_site)
                print("inventory code", inventory_code)
                raise AirflowException("Site is not a number")
            return None

        # print(accessioning[["inventory_code", "site"]].head(5))
        accessioning["site"] = accessioning.apply(
            lambda x: check_site(x["inventory_code"], x["site"]), axis=1
        )
        accessioning["comments"] = accessioning.apply(
            lambda x: str(x["comments"])[:250] if x["comments"] else None, axis=1
        )
        accessioning["status"] = accessioning["status"].replace(STATUS_MAP)
        return accessioning

    def run_ali_validation(aliquot):
        aliquot = aliquot[aliquot["container_type"] != "BloodSpotCard"]
        aliquot = aliquot[
            ~(
                (aliquot["container_type"] == "Micronic 1.4")
                & (aliquot["source"] == "WB")
            )
        ]
        aliquot["specimen_type"] = aliquot["source"].replace(SOURCE_MAPPING)
        aliquot["specimen_type"] = aliquot.apply(
            lambda x: SPECIMEN_MAPPING[x["container_type"].lower()]
            if x["source"] == "WB"
            else x["specimen_type"],
            axis=1,
        )
        miss_stat = aliquot[~aliquot["status"].isin(STATUS_MAP.keys())]
        if miss_stat.shape[0]:
            raise AirflowException(
                f"Incorrect Status (Aliquot): {' ,'.join(miss_stat['inventory_code'])}"
            )
        aliquot["status"] = aliquot["status"].replace(STATUS_MAP)
        return aliquot

    def run_qc_validation(qc):
        vol_unit_dict = {"ml": 1000, "mL": 1000, "uL": 1, "Unit": 10}
        conc_unit_dict = {"ng/ul": 1}

        def add_padding(number):
            if number:
                return f"{round(number, 3):.3f}"
            return None

        def validate_vol(vol_avg, vol_avg_unit, valid_dict):
            if vol_avg is not None and vol_avg_unit is not None:
                return vol_avg * valid_dict[vol_avg_unit]
            elif vol_avg:
                return vol_avg
            return None

        def get_yield(vol, concentration):
            if vol and concentration:
                return vol * concentration / 1000
            return None

        qc = qc.fillna(np.nan).replace([np.nan], [None])
        qc["vol_avg"] = qc.apply(
            lambda x: validate_vol(x.vol_avg, x.volume_unit, vol_unit_dict), axis=1
        )
        qc["vol_avg"] = qc.apply(
            lambda x: 0 if x.vol_avg < 0 else x.vol_avg, axis=1
        ).replace([np.nan], [None])
        qc.loc[~qc["vol_avg"].isnull(), "volume_unit"] = "uL"
        qc.loc[~qc["concentration"].isnull(), "concentration_unit"] = "ng/ul"
        qc["yield"] = qc.apply(
            lambda x: get_yield(x.vol_avg, x.concentration), axis=1
        ).replace([np.nan], [None])
        qc["vol_avg"] = qc.apply(
            lambda x: add_padding(x.vol_avg) if x.vol_avg else None, axis=1
        ).replace([np.nan], [None])
        qc["yield"] = qc.apply(
            lambda x: add_padding(x["yield"]) if x["yield"] else None, axis=1
        ).replace([np.nan], [None])
        qc["concentration"] = qc.apply(
            lambda x: add_padding(x.concentration) if x.concentration else None, axis=1
        ).replace([np.nan], [None])
        return qc

    def run_su_validation(su):
        su["site_name"] = (
            su["site_name"].replace(FACILITY_MAP).replace([np.nan], [None])
        )
        # su = su[(su["site_name"].isnull()) | (su["site_name"].isin(FACILITY_MAP.keys()))] # Temporary Solution
        # su["site_name"] = su.apply(lambda x: "TBD" if x["status"] == "Shipped" and x["site_name"] not in FACILITY_MAP.keys() else x["site_name"], axis = 1)
        # su.loc[(su["status"] == "Shipped") & (su[~su["site_name"].isin(FACILITY_MAP.keys())]), "site_name"] = "TBD"
        # miss_sn = su[(~su["site_name"].isin(FACILITY_MAP.keys())) & (su["status"] == "Shipped")]
        # if miss_sn.shape[0]:
        #     raise AirflowException(f"Incorrect Site Name (Status Updates): {' ,'.join(miss_sn['inventory_code'])}")
        # configure dates depending on what the status is
        # su["Shipped Date"] = su.apply(lambda x: x["date_updated"] if x["status"] == "Shipped" else None, axis = 1)
        # su["Terminal Date"] = su.apply(lambda x: x["date_updated"] if x["status"] == "Disposed" else None, axis = 1)
        return su.sort_values("date_updated").drop_duplicates(
            subset=["inventory_code"], keep="last"
        )

    def unpack_meta(data):
        # print("\n_____________________LIST DATA META___________________\n\n")
        print(list(data["meta"]))
        # print("\n_________________________________________\n")
        meta_data = pd.DataFrame(list(data["meta"]))
        return pd.concat([data.drop(columns=["meta"]), meta_data], axis=1).replace(
            [np.nan], [None]
        )

    @task()
    def fetch_data():
        # Step 1: Get Context for client and project
        context = get_current_context()
        tz = timezone("America/New_York")
        start_time = context["execution_date"]
        file_time = (
            start_time.astimezone(tz=tz).replace(tzinfo=None).strftime("%Y%m%d_%H%M%S")
        )
        # If iterative file, need to check edits to fetch INV CODES edited within the last N days
        print("VALIDATING ACCESSION:\n")
        acc = run_acc_validation(
            unpack_meta(
                pd.read_sql(
                    db.query(Accessioning)
                    .filter(Accessioning.client == "MERCK")
                    .statement,
                    db.bind,
                ).replace([np.nan], [None])
            )
        ).rename(columns=ACC_MAPPING)
        print("DONE\n_________________________________________________\n")
        print("VALIDATING ALIQUOT:\n")
        ali = run_ali_validation(
            unpack_meta(
                pd.read_sql(
                    db.query(Aliquot).filter(Aliquot.client == "MERCK").statement,
                    db.bind,
                ).replace([np.nan], [None])
            )
        ).rename(columns=ALI_MAPPING)
        print("DONE\n_________________________________________________\n")
        print("VALIDATING QUALITY CONTROL:\n")
        qc = run_qc_validation(
            unpack_meta(
                pd.read_sql(
                    db.query(QualityControl)
                    .filter(QualityControl.client == "MERCK")
                    .statement,
                    db.bind,
                ).replace([np.nan], [None])
            )
        ).rename(columns=QC_MAPPING)
        print("DONE\n_________________________________________________\n")
        print("VALIDATING STATUS UPDATE:\n")
        su = run_su_validation(
            pd.read_sql(
                db.query(StatusUpdates)
                .filter(StatusUpdates.client == "MERCK")
                .statement,
                db.bind,
            ).replace([np.nan], [None])
        ).rename(columns=SU_MAPPING)
        print("DONE SU validation and SU data is", su["Terminal Date"])
        # send_data(f"su_df_{file_time}.csv", su)

        # Join Tables Together
        print("Testing Shape: ", acc.shape, ali.shape, qc.shape, su.shape)

        print(
            "\n\n---------------------------------------\nCONC1 = MERGE ALIQUOT AND QC\n\n"
        )
        conc1 = ali.merge(qc, how="inner", on=["Specimen ID"], suffixes=("", "_qc"))
        print("CONC 1: ", conc1.shape)

        conc2 = conc1.merge(
            acc,
            how="inner",
            left_on=["ultimate_parent"],
            right_on=["Specimen ID"],
            suffixes=("", "_acc"),
        )
        print("CONC 2: ", conc2.shape)

        conc3 = pd.concat([conc2, acc], ignore_index=True, sort=False)
        print("CONC 3: ", conc3.shape)

        export = conc3
        export = export.merge(
            su, how="left", on=["Specimen ID", "Current Status"], suffixes=("", "_su")
        )
        print("*****----ZZ")
        print(export[export["Specimen ID"] == "8013398046"])
        print(su[su["Specimen ID"] == "8013398046"])
        print("*****----ZZ")
        send_data(f"export_df_{file_time}.csv", export)
        send_data(f"su_df_{file_time}.csv", su)
        # export = export.merge(su, how = "left", on = ["Specimen ID"], suffixes = ('', '_su'))
        send_data(f"export_su_df_{file_time}.csv", export)

        print("EXPORT : ", export.columns)

        # print("\n\nWriting ACC to CSV\n")
        # send_data(f"Acc_df_{file_time}.csv", acc)
        print("\n\nWriting ALI to CSV\n")
        # send_data(f"ali_df_{file_time}.csv", ali)
        print("\n\nWriting QC to CSV\n")
        # send_data(f"qc_df_{file_time}.csv", qc)
        print("\n\nWriting SU to CSV\n")
        # send_data(f"su_df_{file_time}.csv", su)
        print("\n\nWriting CONC1 ALI+QC to CSV\n")
        # send_data(f"conc1_df_{file_time}.csv", conc1)
        print("\n\nWriting CONC2 ALI+QC+ACC to CSV\n")
        # send_data(f"conc2_df_{file_time}.csv", conc2)
        print("\n\nWriting CONC3 ALI+QC+ACC+ACC to CSV\n")
        # send_data(f"conc3_df_{file_time}.csv", conc3)
        # print("\n\nWriting EXPORT ALI+QC+ACC+ACC+SU to CSV\n")
        # send_data(f"export_df_{file_time}.csv", export)

        print("Testing Shape after joins: ", export.shape)
        export = export.rename(MAPPING)
        export = export[[col for col in ALL_COLUMNS if col in export.columns]]
        export = export.replace([np.nan], [None])

        # Here we will format the dates
        date_columns = [
            "Collection Date",
            "Terminal Date",
            "Shipped Date",
            "Created Date",
            "Received Date",
        ]
        for col in date_columns:
            export[col] = pd.to_datetime(export[col], errors="coerce").dt.strftime(
                "%m/%d/%Y"
            )
        export["Collection Time"] = pd.to_datetime(
            export["Collection Time"], errors="coerce"
        ).dt.strftime("%H:%M")
        export = export.fillna(np.nan).replace([np.nan], [None])

        # print("\n\nWriting EXPORT AFTER REFORMAT to CSV\n")
        # send_data(f"export2_df_{file_time}.csv", export)

        export["Vendor"] = "IBX"
        export["Assay"] = ""
        export["Biopsy Accession ID"] = ""
        export["Biopsy Anatomic Location"] = ""
        export["Biopsy Collection Method"] = ""
        export["Fixation Method"] = ""
        export["Lesion Type"] = ""
        export["Pre/Post Treatment"] = ""
        export["Slide Thickness"] = ""
        export["Slides Sectioned Date"] = ""
        export["Specimen Fixation Date"] = ""
        export["Specimen Tissue Category"] = ""
        export["Diagnosis Confirmed"] = ""
        export["Biopsy Lesion Injection Status"] = ""
        export["Time from Tissue Excision to Immersion in Fixative"] = ""
        export["Fixation Time"] = ""
        export["Institutional Block or Slide ID"] = ""
        export["Time Specimen Placed in Fixative"] = ""
        export["Number of Slides Submitted"] = ""
        export["Type of Biopsy Sample Taken"] = ""
        export["Vendor Specimen ID"] = ""
        print("===========", export["Vendor Specimen ID"])
        export.fillna("")

        # File #1: MAIN
        main_export = export[~export["Study Number"].isin(P3_STUDY)]
        main_inv_export = export[export["Current Status"] == "In Inventory"]
        main_inv_export_nonp3 = main_inv_export[
            ~main_inv_export["Study Number"].isin(P3_STUDY)
        ]
        main_uninvexport = export[export["Current Status"] != "In Inventory"]
        main_uninvexport_nonp3 = main_uninvexport[
            ~main_uninvexport["Study Number"].isin(P3_STUDY)
        ]
        # send_data(f"BioTRACS_Merck_INV_Sampled_{file_time}.csv", main_inv_export)
        send_data(f"BioTRACS_Merck_INV_Sampled_{file_time}.csv", main_inv_export_nonp3)
        # send_data(f"BioTRACS_Merck_NINV_Sampled_{file_time}.csv", main_uninvexport)
        send_data(
            f"BioTRACS_Merck_NINV_Sampled_{file_time}.csv", main_uninvexport_nonp3
        )
        p3_export = export[export["Study Number"].isin(P3_STUDY)]
        p3_inv_export = p3_export[p3_export["Current Status"] == "In Inventory"]
        # p3_inv_export = export[export["Current Status"]=="In Inventory"]
        p3_uninv_export = p3_export[p3_export["Current Status"] != "In Inventory"]
        # p3_uninv_export = export[export["Current Status"]!="In Inventory"]
        send_data(f"BioTRACS_Merck_INV_Sampled_P3_{file_time}.csv", p3_inv_export)
        send_data(f"BioTRACS_Merck_NINV_Sampled_P3_{file_time}.csv", p3_uninv_export)
        # main_export = export[~export["Study Number"].isin(P3_STUDY)]
        # p3_export = export[export["Study Number"].isin(P3_STUDY)]
        # send_data(f"BioTRACS_Merck_INV_Sampled_{file_time}.csv", main_export)
        # send_data(f"BioTRACS_Merck_INV_Sampled_P3_{file_time}.csv", p3_export)
        return True

    fetch_data()


etl = sphere_merck_data_feed_new()
