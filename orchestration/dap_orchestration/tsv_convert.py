#!/usr/bin/env python

import argparse
import csv
import io
import json
import logging
import os
from dataclasses import dataclass, field
from math import ceil
from typing import Optional, Union, Any
from dap_orchestration.types import DapSurveyType

from gcsfs.core import GCSFileSystem

log = logging.getLogger(__name__)

PRIMARY_KEY_PREFIX = 'entity'
TERRA_COLUMN_LIMIT = 1000

TABLES_BY_SURVEY = {
    "hles": ['hles_cancer_condition', 'hles_dog', 'hles_health_condition',
             'hles_owner'],
    "cslb": ["cslb"],
    "environment": ["environment"],
    "sample": ["sample"],
    "eols": ["eols"]
}

DEFAULT_TABLE_NAMES = [table_name for table_names in TABLES_BY_SURVEY.values() for table_name in table_names]


def remove_prefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        return text[len(prefix):]

    return text

# DATA MODELS - Correct Ordering of Columns
def getColumnOrder(table_name):
    if (table_name == "cslb"):
        column_order = ['dog_id', 'cslb_date', 'cslb_year', 'cslb_pace', 'cslb_stare', 'cslb_stuck', 'cslb_recognize', 'cslb_walk_walls', 'cslb_avoid', 'cslb_find_food', 'cslb_pace_6mo', 'cslb_stare_6mo', 'cslb_defecate_6mo', 'cslb_food_6mo', 'cslb_recognize_6mo', 'cslb_active_6mo', 'cslb_score', 'cslb_other_changes']
    if (table_name == "environment"):
        column_order = ['dog_id', 'address_1_or_2', 'address_month', 'address_year', 'gm_address_type', 'gm_match_type', 'gm_state_fips', 'gm_geocoder', 'gm_entry_type', 'gm_complete', 'cv_population_estimate', 'cv_area_sqmi', 'cv_population_density', 'cv_pct_nothispanic_white', 'cv_pct_nothispanic_black', 'cv_pct_nothispanica_ian', 'cv_pct_nothispanic_asian', 'cv_pct_nothispanicn_hpi', 'cv_pct_nothispanic_other', 'cv_pct_nothispanic_two_or_more', 'cv_pct_hispanic', 'cv_pct_female', 'cv_median_income', 'cv_gini_index', 'cv_pct_below_125povline', 'cv_pct_jobless16to64mf', 'cv_pct_famsownchild_female_led', 'cv_pct_less_than_ba_degree', 'cv_pct_less_than_100k', 'cv_disadvantage_index', 'cv_pct_same_house_1yrago', 'cv_pct_owner_occupied', 'cv_pct_us_born', 'cv_stability_index', 'cv_data_year', 'pv_data_year', 'pv_co', 'pv_no2', 'pv_o3', 'pv_pm10', 'pv_pm25', 'pv_so2', 'pv_complete', 'tp_data_year', 'tp_pcpn_annual_01', 'tp_pcpn_annual_02', 'tp_pcpn_annual_03', 'tp_pcpn_annual_04', 'tp_pcpn_annual_05', 'tp_pcpn_annual_06', 'tp_pcpn_annual_07', 'tp_pcpn_annual_08', 'tp_pcpn_annual_09', 'tp_pcpn_annual_10', 'tp_pcpn_annual_11', 'tp_pcpn_annual_12', 'tp_tmpc_annual_01', 'tp_tmpc_annual_02', 'tp_tmpc_annual_03', 'tp_tmpc_annual_04', 'tp_tmpc_annual_05', 'tp_tmpc_annual_06', 'tp_tmpc_annual_07', 'tp_tmpc_annual_08', 'tp_tmpc_annual_09', 'tp_tmpc_annual_10', 'tp_tmpc_annual_11', 'tp_tmpc_annual_12', 'tp_tmax_annual_01', 'tp_tmax_annual_02', 'tp_tmax_annual_03', 'tp_tmax_annual_04', 'tp_tmax_annual_05', 'tp_tmax_annual_06', 'tp_tmax_annual_07', 'tp_tmax_annual_08', 'tp_tmax_annual_09', 'tp_tmax_annual_10', 'tp_tmax_annual_11', 'tp_tmax_annual_12', 'tp_tmin_annual_01', 'tp_tmin_annual_02', 'tp_tmin_annual_03', 'tp_tmin_annual_04', 'tp_tmin_annual_05', 'tp_tmin_annual_06', 'tp_tmin_annual_07', 'tp_tmin_annual_08', 'tp_tmin_annual_09', 'tp_tmin_annual_10', 'tp_tmin_annual_11', 'tp_tmin_annual_12', 'tp_norm_data_year', 'tp_pcpn_norm_01', 'tp_pcpn_norm_02', 'tp_pcpn_norm_03', 'tp_pcpn_norm_04', 'tp_pcpn_norm_05', 'tp_pcpn_norm_06', 'tp_pcpn_norm_07', 'tp_pcpn_norm_08', 'tp_pcpn_norm_09', 'tp_pcpn_norm_10', 'tp_pcpn_norm_11', 'tp_pcpn_norm_12', 'tp_tmpc_norm_01', 'tp_tmpc_norm_02', 'tp_tmpc_norm_03', 'tp_tmpc_norm_04', 'tp_tmpc_norm_05', 'tp_tmpc_norm_06', 'tp_tmpc_norm_07', 'tp_tmpc_norm_08', 'tp_tmpc_norm_09', 'tp_tmpc_norm_10', 'tp_tmpc_norm_11', 'tp_tmpc_norm_12', 'tp_tmax_norm_01', 'tp_tmax_norm_02', 'tp_tmax_norm_03', 'tp_tmax_norm_04', 'tp_tmax_norm_05', 'tp_tmax_norm_06', 'tp_tmax_norm_07', 'tp_tmax_norm_08', 'tp_tmax_norm_09', 'tp_tmax_norm_10', 'tp_tmax_norm_11', 'tp_tmax_norm_12', 'tp_tmin_norm_01', 'tp_tmin_norm_02', 'tp_tmin_norm_03', 'tp_tmin_norm_04', 'tp_tmin_norm_05', 'tp_tmin_norm_06', 'tp_tmin_norm_07', 'tp_tmin_norm_08', 'tp_tmin_norm_09', 'tp_tmin_norm_10', 'tp_tmin_norm_11', 'tp_tmin_norm_12', 'wv_walkscore', 'wv_walkscore_descrip', 'wv_walkscore_date', 'wv_housing_units', 'wv_res_density', 'wv_density_data_year']
    if (table_name == "sample"):
        column_order = ['sample_id', 'dog_id', 'cohort', 'sample_type', 'date_swab_arrival_laboratory']
    if (table_name == "hles_owner"):
        column_order = ['owner_id', 'od_age_range_years', 'od_max_education', 'od_race_white', 'od_race_black_or_african_american', 'od_race_asian', 'od_race_american_indian', 'od_race_alaska_native', 'od_race_native_hawaiian', 'od_race_other_pacific_islander', 'od_race_other', 'od_race_other_description', 'od_hispanic', 'od_annual_income_range_usd', 'oc_household_person_count', 'oc_household_adult_count', 'oc_household_child_count', 'ss_household_dog_count', 'oc_primary_residence_state', 'oc_primary_residence_census_division', 'oc_primary_residence_ownership', 'oc_primary_residence_ownership_other_description', 'oc_primary_residence_time_percentage', 'oc_secondary_residence', 'oc_secondary_residence_state', 'oc_secondary_residence_ownership', 'oc_secondary_residence_ownership_other_description', 'oc_secondary_residence_time_percentage']
    if (table_name == "hles_cancer_condition"):
        column_order = ['dog_id', 'hs_initial_diagnosis_year', 'hs_initial_diagnosis_month', 'hs_required_surgery_or_hospitalization', 'hs_follow_up_ongoing', 'hs_cancer_locations_adrenal_gland', 'hs_cancer_locations_anal_sac', 'hs_cancer_locations_bladder_or_urethra', 'hs_cancer_locations_blood', 'hs_cancer_locations_bone_or_joint', 'hs_cancer_locations_brain', 'hs_cancer_locations_mammary_tissue', 'hs_cancer_locations_cardiac_tissue', 'hs_cancer_locations_ear', 'hs_cancer_locations_esophagus', 'hs_cancer_locations_eye', 'hs_cancer_locations_gallbladder_or_bile_duct', 'hs_cancer_locations_gastrointestinal_tract', 'hs_cancer_locations_kidney', 'hs_cancer_locations_liver', 'hs_cancer_locations_lung', 'hs_cancer_locations_lymph_nodes', 'hs_cancer_locations_muscle_or_soft_tissue', 'hs_cancer_locations_nose_or_nasal_passage', 'hs_cancer_locations_nerve_sheath', 'hs_cancer_locations_oral_cavity', 'hs_cancer_locations_ovary_or_uterus', 'hs_cancer_locations_pancreas', 'hs_cancer_locations_perianal_area', 'hs_cancer_locations_pituitary_gland', 'hs_cancer_locations_prostate', 'hs_cancer_locations_rectum', 'hs_cancer_locations_skin_of_trunk_body_head', 'hs_cancer_locations_skin_of_limb_or_foot', 'hs_cancer_locations_spinal_cord', 'hs_cancer_locations_spleen', 'hs_cancer_locations_testicle', 'hs_cancer_locations_thyroid', 'hs_cancer_locations_venereal', 'hs_cancer_locations_unknown', 'hs_cancer_locations_other', 'hs_cancer_locations_other_description', 'hs_cancer_types_adenoma', 'hs_cancer_types_adenocarcinoma', 'hs_cancer_types_basal_cell_tumor', 'hs_cancer_types_carcinoma', 'hs_cancer_types_chondrosarcoma', 'hs_cancer_types_cystadenoma', 'hs_cancer_types_epidermoid_cyst', 'hs_cancer_types_epulides', 'hs_cancer_types_fibrosarcoma', 'hs_cancer_types_hemangioma', 'hs_cancer_types_hemangiosarcoma', 'hs_cancer_types_histiocytic_sarcoma', 'hs_cancer_types_histiocytoma', 'hs_cancer_types_insulinoma', 'hs_cancer_types_leukemia', 'hs_cancer_types_leiomyoma', 'hs_cancer_types_leiomyosarcoma', 'hs_cancer_types_lipoma', 'hs_cancer_types_lymphoma_lymphosarcoma', 'hs_cancer_types_mast_cell_tumor', 'hs_cancer_types_melanoma', 'hs_cancer_types_meningioma', 'hs_cancer_types_multiple_myeloma', 'hs_cancer_types_osteosarcoma', 'hs_cancer_types_papilloma', 'hs_cancer_types_peripheral_nerve_sheath_tumor', 'hs_cancer_types_plasmacytoma', 'hs_cancer_types_rhabdomyosarcoma', 'hs_cancer_types_sarcoma', 'hs_cancer_types_sebaceous_adenoma', 'hs_cancer_types_soft_tissue_sarcoma', 'hs_cancer_types_squamous_cell_carcinoma', 'hs_cancer_types_thymoma', 'hs_cancer_types_transitional_cell_carcinoma', 'hs_cancer_types_unknown', 'hs_cancer_types_other', 'hs_cancer_types_other_description', 'hs_leukemia_types_acute', 'hs_leukemia_types_chronic', 'hs_leukemia_types_unknown', 'hs_leukemia_types_other', 'hs_leukemia_types_other_description', 'hs_lymphoma_lymphosarcoma_types_b_cell', 'hs_lymphoma_lymphosarcoma_types_t_cell', 'hs_lymphoma_lymphosarcoma_types_t_zone', 'hs_lymphoma_lymphosarcoma_types_unknown', 'hs_lymphoma_lymphosarcoma_types_other', 'hs_lymphoma_lymphosarcoma_types_other_description']
    if (table_name == "hles_health_condition"):
        column_order = ['dog_id', 'hs_condition_type', 'hs_condition', 'hs_condition_other_description', 'hs_condition_is_congenital', 'hs_eye_condition_cause', 'hs_condition_cause_other_description', 'hs_neurological_condition_vestibular_disease_type', 'hs_diagnosis_year', 'hs_diagnosis_month', 'hs_required_surgery_or_hospitalization', 'hs_follow_up_ongoing']
    if (table_name == "eols"):
        column_order = ['dog_id', 'eol_willing_to_complete', 'eol_exact_death_date_known', 'eol_death_date', 'eol_cause_of_death_primary', 'eol_cause_of_death_primary_other_description', 'eol_cause_of_death_secondary', 'eol_cause_of_death_secondary_other_description', 'eol_old_age_primary', 'eol_old_age_primary_other_description', 'eol_trauma', 'eol_trauma_other_description', 'eol_toxin', 'eol_toxin_other_description', 'eol_old_age_secondary', 'eol_old_age_secondary_other_description', 'eol_notes_description', 'eol_add_vemr', 'eol_new_condition_none', 'eol_new_condition_infectious_disease', 'eol_new_condition_toxin_consumption', 'eol_new_condition_trauma', 'eol_new_condition_cancer', 'eol_new_condition_eye', 'eol_new_condition_ear', 'eol_new_condition_oral', 'eol_new_condition_skin', 'eol_new_condition_cardiac', 'eol_new_condition_respiratory', 'eol_new_condition_gastrointestinal', 'eol_new_condition_liver', 'eol_new_condition_kidney', 'eol_new_condition_reproductive', 'eol_new_condition_orthopedic', 'eol_new_condition_neurological', 'eol_new_condition_endocrine', 'eol_new_condition_homatologic', 'eol_new_condition_immune', 'eol_new_condition_other', 'eol_new_condition_infectious_disease_month', 'eol_new_condition_infectious_disease_year', 'eol_new_condition_infectious_disease_specify', 'eol_new_condition_toxin_consumption_month', 'eol_new_condition_toxin_consumption_year', 'eol_new_condition_toxin_consumption_specify', 'eol_new_condition_trauma_month', 'eol_new_condition_trauma_year', 'eol_new_condition_trauma_specify', 'eol_new_condition_cancer_month', 'eol_new_condition_cancer_year', 'eol_new_condition_cancer_specify', 'eol_new_condition_eye_month', 'eol_new_condition_eye_year', 'eol_new_condition_eye_specify', 'eol_new_condition_ear_month', 'eol_new_condition_ear_year', 'eol_new_condition_ear_specify', 'eol_new_condition_oral_month', 'eol_new_condition_oral_year', 'eol_new_condition_oral_specify', 'eol_new_condition_skin_month', 'eol_new_condition_skin_year', 'eol_new_condition_skin_specify', 'eol_new_condition_cardiac_month', 'eol_new_condition_cardiac_year', 'eol_new_condition_cardiac_specify', 'eol_new_condition_respiratory_month', 'eol_new_condition_respiratory_year', 'eol_new_condition_respiratory_specify', 'eol_new_condition_gastrointestinal_month', 'eol_new_condition_gastrointestinal_year', 'eol_new_condition_gastrointestinal_specify', 'eol_new_condition_liver_month', 'eol_new_condition_liver_year', 'eol_new_condition_liver_specify', 'eol_new_condition_kidney_month', 'eol_new_condition_kidney_year', 'eol_new_condition_kidney_specify', 'eol_new_condition_reproductive_month', 'eol_new_condition_reproductive_year', 'eol_new_condition_reproductive_specify', 'eol_new_condition_orthopedic_month', 'eol_new_condition_orthopedic_year', 'eol_new_condition_orthopedic_specify', 'eol_new_condition_neurological_month', 'eol_new_condition_neurological_year', 'eol_new_condition_neurological_specify', 'eol_new_condition_endocrine_month', 'eol_new_condition_endocrine_year', 'eol_new_condition_endocrine_specify', 'eol_new_condition_homatologic_month', 'eol_new_condition_homatologic_year', 'eol_new_condition_homatologic_specify', 'eol_new_condition_immune_month', 'eol_new_condition_immune_year', 'eol_new_condition_immune_specify', 'eol_new_condition_other_month', 'eol_new_condition_other_year', 'eol_new_condition_other_specify', 'eol_recent_aging_char_none', 'eol_recent_aging_char_blind', 'eol_recent_aging_char_deaf', 'eol_recent_aging_char_weightloss', 'eol_recent_aging_char_mobility_weak', 'eol_recent_aging_char_mobility_pain', 'eol_recent_aging_char_other_pain', 'eol_recent_aging_char_cleanliness', 'eol_recent_aging_char_confusion', 'eol_recent_aging_char_interaction_change', 'eol_recent_aging_char_sleep', 'eol_recent_aging_char_housesoiling', 'eol_recent_aging_char_anxiety', 'eol_recent_aging_char_eat_drink', 'eol_recent_aging_char_inactivity', 'eol_recent_aging_char_repetitive_activity', 'eol_recent_aging_char_memory', 'eol_recent_aging_char_recognition', 'eol_recent_aging_char_sundowning', 'eol_recent_aging_char_other', 'eol_recent_aging_char_other_description', 'eol_recent_symptom_none', 'eol_recent_symptom_lethargy', 'eol_recent_symptom_vomiting', 'eol_recent_symptom_diarrhea', 'eol_recent_symptom_appetite', 'eol_recent_symptom_weight', 'eol_recent_symptom_coughing', 'eol_recent_symptom_sneezing', 'eol_recent_symptom_breathing', 'eol_recent_symptom_bleeding_bruising', 'eol_recent_symptom_drink_lot', 'eol_recent_symptom_drink_little', 'eol_recent_symptom_urinating', 'eol_recent_symptom_incontinence', 'eol_recent_symptom_sores', 'eol_recent_symptom_seizures', 'eol_recent_symptom_swolen_abdomen', 'eol_recent_symptom_other', 'eol_recent_symptom_other_description', 'eol_recent_qol', 'eol_qol_declined', 'eol_qol_declined_reason', 'eol_qol_declined_reason_other_description', 'eol_recent_vet_discuss', 'eol_recent_vet_stay', 'eol_recent_vet_stay_length', 'eol_recent_sedation', 'eol_understand_prognosis', 'eol_death_location', 'eol_death_location_other_description', 'eol_death_witness', 'eol_death_witness_who_you', 'eol_death_witness_who_family', 'eol_death_witness_who_acquaintance', 'eol_death_witness_who_vet', 'eol_death_witness_who_boarder', 'eol_death_witness_who_other', 'eol_death_witness_who_other_description', 'eol_euthan', 'eol_euthan_who', 'eol_euthan_who_other_description', 'eol_euthan_main_reason', 'eol_euthan_main_reason_other_description', 'eol_euthan_add_reason_none', 'eol_euthan_add_reason_quality_of_life', 'eol_euthan_add_reason_pain', 'eol_euthan_add_reason_prognosis', 'eol_euthan_add_reason_medic_prob', 'eol_euthan_add_reason_behavior_prob', 'eol_euthan_add_reason_harm_to_another', 'eol_euthan_add_reason_incompatible', 'eol_euthan_add_reason_cost', 'eol_euthan_add_reason_other', 'eol_euthan_add_reason_other_description', 'eol_illness_type', 'eol_illness_cancer_adrenal', 'eol_illness_cancer_anal_sac', 'eol_illness_cancer_bladder_urethra', 'eol_illness_cancer_blood', 'eol_illness_cancer_bone_joint', 'eol_illness_cancer_brain', 'eol_illness_cancer_mammary', 'eol_illness_cancer_cardiac', 'eol_illness_cancer_ear', 'eol_illness_cancer_esophagus', 'eol_illness_cancer_eye', 'eol_illness_cancer_gallbladder', 'eol_illness_cancer_gastro', 'eol_illness_cancer_kidney', 'eol_illness_cancer_liver', 'eol_illness_cancer_lung', 'eol_illness_cancer_lymph_nodes', 'eol_illness_cancer_muscle', 'eol_illness_cancer_nose', 'eol_illness_cancer_nerve_sheath', 'eol_illness_cancer_oral', 'eol_illness_cancer_ovary_uterus', 'eol_illness_cancer_pancreas', 'eol_illness_cancer_perianal', 'eol_illness_cancer_pituitary', 'eol_illness_cancer_prostate', 'eol_illness_cancer_rectum', 'eol_illness_cancer_skin_trunk_body_head', 'eol_illness_cancer_skin_limb_food', 'eol_illness_cancer_spinal_cord', 'eol_illness_cancer_spleen', 'eol_illness_cancer_testicle', 'eol_illness_cancer_thyroid', 'eol_illness_cancer_venereal', 'eol_illness_cancer_unidentified_thorax', 'eol_illness_cancer_unidentified_abdomen', 'eol_illness_cancer_unknown', 'eol_illness_cancer_other', 'eol_illness_cancer_other_description', 'eol_illness_cancer_name_known', 'eol_illness_cancer_name_description', 'eol_illness_infection', 'eol_illness_infection_other_description', 'eol_illness_infection_system', 'eol_illness_infection_system_other_description', 'eol_illness_other', 'eol_illness_other_other_description', 'eol_illness_other_diagnosis', 'eol_illness_other_diagnosis_description', 'eol_illness_awareness_timeframe', 'eol_illness_treatment', 'eol_illness_treatment_other_description']
    if (table_name == "hles_dog"):
        column_order = ['dog_id', 'owner_id', 'st_vip_or_staff', 'st_portal_invitation_date', 'st_portal_account_creation_date', 'st_hles_completion_date', 'dd_breed_pure_or_mixed', 'dd_breed_pure', 'dd_breed_pure_non_akc', 'dd_breed_mixed_primary', 'dd_breed_mixed_secondary', 'dd_age_years', 'dd_age_basis', 'dd_age_exact_source_acquired_as_puppy', 'dd_age_exact_source_registration_information', 'dd_age_exact_source_determined_by_rescue_org', 'dd_age_exact_source_determined_by_veterinarian', 'dd_age_exact_source_from_litter_owner_bred', 'dd_age_exact_source_other', 'dd_age_exact_source_other_description', 'dd_age_estimate_source_told_by_previous_owner', 'dd_age_estimate_source_determined_by_rescue_org', 'dd_age_estimate_source_determined_by_veterinarian', 'dd_age_estimate_source_other', 'dd_age_estimate_source_other_description', 'dd_birth_year', 'dd_birth_month_known', 'dd_birth_month', 'dd_sex', 'dd_spayed_or_neutered', 'dd_spay_or_neuter_age', 'dd_spay_method', 'dd_estrous_cycle_experienced_before_spayed', 'dd_estrous_cycle_count', 'dd_has_been_pregnant', 'dd_has_sired_litters', 'dd_litter_count', 'dd_weight_range', 'dd_weight_lbs', 'dd_weight_range_expected_adult', 'dd_insurance', 'dd_insurance_provider', 'dd_insurance_provider_other_description', 'dd_acquired_year', 'dd_acquired_month', 'dd_acquired_source', 'dd_acquired_source_other_description', 'dd_acquired_country', 'dd_acquired_state', 'dd_activities_companion_animal', 'dd_activities_obedience', 'dd_activities_show', 'dd_activities_breeding', 'dd_activities_agility', 'dd_activities_hunting', 'dd_activities_working', 'dd_activities_field_trials', 'dd_activities_search_and_rescue', 'dd_activities_service', 'dd_activities_assistance_or_therapy', 'dd_activities_other', 'dd_activities_other_description', 'dd_activities_service_seeing_eye', 'dd_activities_service_hearing_or_signal', 'dd_activities_service_wheelchair', 'dd_activities_service_other_medical', 'dd_activities_service_other_medical_description', 'dd_activities_service_other_health', 'dd_activities_service_other_health_description', 'dd_activities_service_community_therapy', 'dd_activities_service_emotional_support', 'dd_activities_service_other', 'dd_activities_service_other_description', 'dd_alternate_recent_residence_count', 'dd_alternate_recent_residence1_state', 'dd_alternate_recent_residence1_weeks', 'dd_alternate_recent_residence2_state', 'dd_alternate_recent_residence2_weeks', 'dd_alternate_recent_residence3_state', 'dd_alternate_recent_residence3_weeks', 'dd_alternate_recent_residence4_state', 'dd_alternate_recent_residence4_weeks', 'dd_alternate_recent_residence5_state', 'dd_alternate_recent_residence5_weeks', 'dd_alternate_recent_residence6_state', 'dd_alternate_recent_residence6_weeks', 'dd_alternate_recent_residence7_state', 'dd_alternate_recent_residence7_weeks', 'dd_alternate_recent_residence8_state', 'dd_alternate_recent_residence8_weeks', 'dd_alternate_recent_residence9_state', 'dd_alternate_recent_residence9_weeks', 'dd_alternate_recent_residence10_state', 'dd_alternate_recent_residence10_weeks', 'dd_alternate_recent_residence11_state', 'dd_alternate_recent_residence11_weeks', 'dd_alternate_recent_residence12_state', 'dd_alternate_recent_residence12_weeks', 'dd_alternate_recent_residence13_state', 'dd_alternate_recent_residence13_weeks', 'pa_activity_level', 'pa_avg_daily_active_hours', 'pa_avg_daily_active_minutes', 'pa_avg_activity_intensity', 'pa_moderate_weather_months_per_year', 'pa_moderate_weather_daily_hours_outside', 'pa_moderate_weather_outdoor_concrete', 'pa_moderate_weather_outdoor_wood', 'pa_moderate_weather_outdoor_other_hard_surface', 'pa_moderate_weather_outdoor_grass_or_dirt', 'pa_moderate_weather_outdoor_gravel', 'pa_moderate_weather_outdoor_sand', 'pa_moderate_weather_outdoor_astroturf', 'pa_moderate_weather_outdoor_other_surface', 'pa_moderate_weather_outdoor_other_surface_description', 'pa_moderate_weather_sun_exposure_level', 'pa_hot_weather_months_per_year', 'pa_hot_weather_daily_hours_outside', 'pa_hot_weather_outdoor_concrete', 'pa_hot_weather_outdoor_wood', 'pa_hot_weather_outdoor_other_hard_surface', 'pa_hot_weather_outdoor_grass_or_dirt', 'pa_hot_weather_outdoor_gravel', 'pa_hot_weather_outdoor_sand', 'pa_hot_weather_outdoor_astroturf', 'pa_hot_weather_outdoor_other_surface', 'pa_hot_weather_outdoor_other_surface_description', 'pa_hot_weather_sun_exposure_level', 'pa_cold_weather_months_per_year', 'pa_cold_weather_daily_hours_outside', 'pa_cold_weather_outdoor_concrete', 'pa_cold_weather_outdoor_wood', 'pa_cold_weather_outdoor_other_hard_surface', 'pa_cold_weather_outdoor_grass_or_dirt', 'pa_cold_weather_outdoor_gravel', 'pa_cold_weather_outdoor_sand', 'pa_cold_weather_outdoor_astroturf', 'pa_cold_weather_outdoor_other_surface', 'pa_cold_weather_outdoor_other_surface_description', 'pa_cold_weather_sun_exposure_level', 'pa_on_leash_off_leash_walk', 'pa_on_leash_walk_frequency', 'pa_on_leash_walk_avg_hours', 'pa_on_leash_walk_avg_minutes', 'pa_on_leash_walk_slow_pace_pct', 'pa_on_leash_walk_average_pace_pct', 'pa_on_leash_walk_brisk_pace_pct', 'pa_on_leash_walk_jog_pace_pct', 'pa_on_leash_walk_run_pace_pct', 'pa_on_leash_walk_reasons_dog_relieve_itself', 'pa_on_leash_walk_reasons_activity_and_enjoyment', 'pa_on_leash_walk_reasons_exercise_for_dog', 'pa_on_leash_walk_reasons_exercise_for_owner', 'pa_on_leash_walk_reasons_training_obedience', 'pa_on_leash_walk_reasons_other', 'pa_on_leash_walk_reasons_other_description', 'pa_off_leash_walk_frequency', 'pa_off_leash_walk_avg_hours', 'pa_off_leash_walk_avg_minutes', 'pa_off_leash_walk_slow_pace_pct', 'pa_off_leash_walk_average_pace_pct', 'pa_off_leash_walk_brisk_pace_pct', 'pa_off_leash_walk_jog_pace_pct', 'pa_off_leash_walk_run_pace_pct', 'pa_off_leash_walk_reasons_dog_relieve_itself', 'pa_off_leash_walk_reasons_activity_and_enjoyment', 'pa_off_leash_walk_reasons_exercise_for_dog', 'pa_off_leash_walk_reasons_exercise_for_owner', 'pa_off_leash_walk_reasons_training_obedience', 'pa_off_leash_walk_reasons_other', 'pa_off_leash_walk_reasons_other_description', 'pa_off_leash_walk_in_enclosed_area', 'pa_off_leash_walk_in_open_area', 'pa_off_leash_walk_returns_when_called_frequency', 'pa_physical_games_frequency', 'pa_swim', 'pa_swim_moderate_weather_frequency', 'pa_swim_hot_weather_frequency', 'pa_swim_cold_weather_frequency', 'pa_swim_locations_swimming_pool', 'pa_swim_locations_pond_or_lake', 'pa_swim_locations_river_stream_or_creek', 'pa_swim_locations_agricultural_ditch', 'pa_swim_locations_ocean', 'pa_swim_locations_other', 'pa_swim_locations_other_description', 'pa_other_aerobic_activity_frequency', 'pa_other_aerobic_activity_avg_hours', 'pa_other_aerobic_activity_avg_minutes', 'pa_other_aerobic_activity_avg_intensity', 'de_lifetime_residence_count', 'de_past_residence_zip_count', 'de_past_residence_country_count', 'de_past_residence_country1', 'de_past_residence_country2', 'de_past_residence_country3', 'de_past_residence_country4', 'de_past_residence_country5', 'de_past_residence_country6', 'de_past_residence_country7', 'de_past_residence_country8', 'de_past_residence_country9', 'de_past_residence_country10', 'de_past_residence_country1_text', 'de_past_residence_country2_text', 'de_home_area_type', 'de_home_type', 'de_home_type_other_description', 'de_home_construction_decade', 'de_home_years_lived_in', 'de_home_square_footage', 'de_primary_heat_fuel', 'de_primary_heat_fuel_other_description', 'de_secondary_heat_fuel_used', 'de_secondary_heat_fuel', 'de_secondary_heat_fuel_other_description', 'de_primary_stove_fuel', 'de_primary_stove_fuel_other_description', 'de_secondary_stove_fuel_used', 'de_secondary_stove_fuel', 'de_secondary_stove_fuel_other_description', 'de_drinking_water_source', 'de_drinking_water_source_other_description', 'de_drinking_water_is_filtered', 'de_pipe_type', 'de_pipe_type_other_description', 'de_second_hand_smoke_hours_per_day', 'de_central_air_conditioning_present', 'de_room_or_window_air_conditioning_present', 'de_central_heat_present', 'de_asbestos_present', 'de_radon_present', 'de_lead_present', 'de_mothball_present', 'de_incense_present', 'de_air_freshener_present', 'de_air_cleaner_present', 'de_hepa_present', 'de_wood_fireplace_present', 'de_gas_fireplace_present', 'de_wood_fireplace_lightings_per_week', 'de_gas_fireplace_lightings_per_week', 'de_floor_types_wood', 'de_floor_frequency_on_wood', 'de_floor_types_carpet', 'de_floor_frequency_on_carpet', 'de_floor_types_concrete', 'de_floor_frequency_on_concrete', 'de_floor_types_tile', 'de_floor_frequency_on_tile', 'de_floor_types_linoleum', 'de_floor_frequency_on_linoleum', 'de_floor_types_laminate', 'de_floor_frequency_on_laminate', 'de_floor_types_other', 'de_floor_types_other_description', 'de_floor_frequency_on_other', 'de_stairs_in_home', 'de_stairs_avg_flights_per_day', 'de_property_area', 'de_property_accessible', 'de_property_area_accessible', 'de_property_containment_type', 'de_property_containment_type_other_description', 'de_property_drinking_water_none', 'de_property_drinking_water_bowl', 'de_property_drinking_water_hose', 'de_property_drinking_water_puddles', 'de_property_drinking_water_unknown', 'de_property_drinking_water_other', 'de_property_drinking_water_other_description', 'de_property_weed_control_frequency', 'de_property_pest_control_frequency', 'de_traffic_noise_in_home_frequency', 'de_traffic_noise_in_property_frequency', 'de_neighborhood_has_sidewalks', 'de_neighborhood_has_parks', 'de_interacts_with_neighborhood_animals', 'de_interacts_with_neighborhood_animals_with_owner', 'de_interacts_with_neighborhood_humans', 'de_interacts_with_neighborhood_humans_with_owner', 'de_dogpark', 'de_dogpark_days_per_month', 'de_dogpark_travel_walk', 'de_dogpark_travel_drive', 'de_dogpark_travel_bike', 'de_dogpark_travel_public_transportation', 'de_dogpark_travel_other', 'de_dogpark_travel_other_description', 'de_dogpark_travel_time_hours', 'de_dogpark_travel_time_minutes', 'de_recreational_spaces', 'de_recreational_spaces_days_per_month', 'de_recreational_spaces_travel_walk', 'de_recreational_spaces_travel_drive', 'de_recreational_spaces_travel_bike', 'de_recreational_spaces_travel_public_transportation', 'de_recreational_spaces_travel_other', 'de_recreational_spaces_travel_other_description', 'de_recreational_spaces_travel_time_hours', 'de_recreational_spaces_travel_time_minutes', 'de_work', 'de_work_days_per_month', 'de_work_travel_walk', 'de_work_travel_drive', 'de_work_travel_bike', 'de_work_travel_public_transportation', 'de_work_travel_other', 'de_work_travel_other_description', 'de_work_travel_time_hours', 'de_work_travel_time_minutes', 'de_sitter_or_daycare', 'de_sitter_or_daycare_days_per_month', 'de_sitter_or_daycare_travel_walk', 'de_sitter_or_daycare_travel_drive', 'de_sitter_or_daycare_travel_bike', 'de_sitter_or_daycare_travel_public_transportation', 'de_sitter_or_daycare_travel_other', 'de_sitter_or_daycare_travel_other_description', 'de_sitter_or_daycare_travel_time_hours', 'de_sitter_or_daycare_travel_time_minutes', 'de_eats_grass_frequency', 'de_eats_feces', 'de_eats_feces_own_feces', 'de_eats_feces_other_dog', 'de_eats_feces_cat', 'de_eats_feces_horse', 'de_eats_feces_cattle', 'de_eats_feces_wildlife', 'de_eats_feces_other', 'de_eats_feces_other_description', 'de_drinks_outdoor_water', 'de_drinks_outdoor_water_frequency', 'de_routine_toys', 'de_routine_toys_include_plastic', 'de_routine_toys_include_fabric_stuffed', 'de_routine_toys_include_fabric_unstuffed', 'de_routine_toys_include_rubber', 'de_routine_toys_include_metal', 'de_routine_toys_include_animal_products', 'de_routine_toys_include_latex', 'de_routine_toys_include_rope', 'de_routine_toys_include_tennis_balls', 'de_routine_toys_include_sticks', 'de_routine_toys_include_other', 'de_routine_toys_other_description', 'de_routine_toys_hours_per_day', 'de_nighttime_sleep_location', 'de_nighttime_sleep_location_other_description', 'de_nighttime_sleep_avg_hours', 'de_daytime_sleep_location_different', 'de_daytime_sleep_location', 'de_daytime_sleep_location_other_description', 'de_daytime_sleep_avg_hours', 'de_licks_chews_or_plays_with_non_toys', 'de_recent_toxins_or_hazards_ingested_frequency', 'de_recent_toxins_or_hazards_ingested_chocolate', 'de_recent_toxins_or_hazards_ingested_poison', 'de_recent_toxins_or_hazards_ingested_human_medication', 'de_recent_toxins_or_hazards_ingested_pet_medication', 'de_recent_toxins_or_hazards_ingested_garbage_or_food', 'de_recent_toxins_or_hazards_ingested_dead_animal', 'de_recent_toxins_or_hazards_ingested_toys', 'de_recent_toxins_or_hazards_ingested_clothing', 'de_recent_toxins_or_hazards_ingested_other', 'de_recent_toxins_or_hazards_ingested_other_description', 'de_recent_toxins_or_hazards_ingested_required_vet', 'de_other_present_animals', 'de_other_present_animals_dogs', 'de_other_present_animals_cats', 'de_other_present_animals_birds', 'de_other_present_animals_reptiles', 'de_other_present_animals_livestock', 'de_other_present_animals_horses', 'de_other_present_animals_rodents', 'de_other_present_animals_fish', 'de_other_present_animals_wildlife', 'de_other_present_animals_other', 'de_other_present_animals_other_description', 'de_other_present_animals_indoor_count', 'de_other_present_animals_outdoor_count', 'de_other_present_animals_interact_with_dog', 'de_routine_consistency', 'de_routine_hours_per_day_in_crate', 'de_routine_hours_per_day_roaming_house', 'de_routine_hours_per_day_in_garage', 'de_routine_hours_per_day_in_outdoor_kennel', 'de_routine_hours_per_day_in_yard', 'de_routine_hours_per_day_roaming_outside', 'de_routine_hours_per_day_chained_outside', 'de_routine_hours_per_day_away_from_home', 'de_routine_hours_per_day_with_other_animals', 'de_routine_hours_per_day_with_adults', 'de_routine_hours_per_day_with_teens', 'de_routine_hours_per_day_with_children', 'db_excitement_level_before_walk', 'db_excitement_level_before_car_ride', 'db_aggression_level_on_leash_unknown_human', 'db_aggression_level_toys_taken_away', 'db_aggression_level_approached_while_eating', 'db_aggression_level_delivery_workers_at_home', 'db_aggression_level_food_taken_away', 'db_aggression_level_on_leash_unknown_dog', 'db_aggression_level_unknown_human_near_yard', 'db_aggression_level_unknown_aggressive_dog', 'db_aggression_level_familiar_dog_while_eating', 'db_aggression_level_familiar_dog_while_playing', 'db_fear_level_unknown_human_away_from_home', 'db_fear_level_loud_noises', 'db_fear_level_unknown_human_touch', 'db_fear_level_unknown_objects_outside', 'db_fear_level_unknown_dogs', 'db_fear_level_unknown_situations', 'db_fear_level_unknown_aggressive_dog', 'db_fear_level_nails_clipped_at_home', 'db_fear_level_bathed_at_home', 'db_left_alone_restlessness_frequency', 'db_left_alone_barking_frequency', 'db_left_alone_scratching_frequency', 'db_attention_seeking_follows_humans_frequency', 'db_attention_seeking_sits_close_to_humans_frequency', 'db_training_obeys_sit_command_frequency', 'db_training_obeys_stay_command_frequency', 'db_training_distraction_frequency', 'db_chases_birds_frequency', 'db_chases_squirrels_frequency', 'db_escapes_home_or_property_frequency', 'db_chews_inappropriate_objects_frequency', 'db_pulls_leash_frequency', 'db_urinates_in_home_frequency', 'db_urinates_alone_frequency', 'db_defecates_alone_frequency', 'db_hyperactive_frequency', 'db_playful_frequency', 'db_energetic_frequency', 'db_chases_tail_frequency', 'db_barks_frequency', 'df_feedings_per_day', 'df_primary_diet_component', 'df_primary_diet_component_other_description', 'df_primary_diet_component_organic', 'df_primary_diet_component_grain_free', 'df_primary_diet_component_grain_free_past', 'df_primary_diet_component_change_recent', 'df_primary_diet_component_change_months_ago', 'df_primary_diet_component_change_allergy_related', 'df_primary_diet_component_change_different_life_stage', 'df_primary_diet_component_change_stop_grain_free', 'df_primary_diet_component_change_health_condition_specific', 'df_primary_diet_component_change_brand_change', 'df_primary_diet_component_change_new_food_same_brand', 'df_primary_diet_component_change_other', 'df_primary_diet_component_change_other_description', 'df_secondary_diet_component_used', 'df_secondary_diet_component', 'df_secondary_diet_component_other_description', 'df_secondary_diet_component_organic', 'df_secondary_diet_component_grain_free', 'df_secondary_diet_component_grain_free_past', 'df_secondary_diet_component_change_recent', 'df_secondary_diet_component_change_months_ago', 'df_secondary_diet_component_change_allergy_related', 'df_secondary_diet_component_change_different_life_stage', 'df_secondary_diet_component_change_stop_grain_free', 'df_secondary_diet_component_change_health_condition_specific', 'df_secondary_diet_component_change_brand_change', 'df_secondary_diet_component_change_new_food_same_brand', 'df_secondary_diet_component_change_other', 'df_secondary_diet_component_change_other_description', 'df_treats_frequency', 'df_treats_commercial_biscuits', 'df_treats_rawhide', 'df_treats_bones', 'df_treats_table_meat', 'df_treats_table_carbs', 'df_treats_vegetables', 'df_treats_homemade_protein', 'df_treats_homemade_biscuits', 'df_treats_pumpkin', 'df_treats_peanut_butter', 'df_treats_other', 'df_treats_other_description', 'df_daily_supplements', 'df_daily_supplements_bone_meal', 'df_daily_supplements_glucosamine', 'df_daily_supplements_chondroitin', 'df_daily_supplements_other_joint', 'df_daily_supplements_omega3', 'df_daily_supplements_non_oil_skin', 'df_daily_supplements_vitamins', 'df_daily_supplements_enzyme', 'df_daily_supplements_probiotics', 'df_daily_supplements_fiber', 'df_daily_supplements_alkalinize', 'df_daily_supplements_acidify', 'df_daily_supplements_taurine', 'df_daily_supplements_antiox', 'df_daily_supplements_coenzyme_q10', 'df_daily_supplements_other', 'df_daily_supplements_other_description', 'df_infrequent_supplements', 'df_infrequent_supplements_bone_meal', 'df_infrequent_supplements_glucosamine', 'df_infrequent_supplements_chondroitin', 'df_infrequent_supplements_other_joint', 'df_infrequent_supplements_omega3', 'df_infrequent_supplements_non_oil_skin', 'df_infrequent_supplements_vitamins', 'df_infrequent_supplements_enzyme', 'df_infrequent_supplements_probiotics', 'df_infrequent_supplements_fiber', 'df_infrequent_supplements_alkalinize', 'df_infrequent_supplements_acidify', 'df_infrequent_supplements_taurine', 'df_infrequent_supplements_antiox', 'df_infrequent_supplements_coenzyme_q10', 'df_infrequent_supplements_other', 'df_infrequent_supplements_other_description', 'df_diet_consistency', 'df_diet_consistency_other_description', 'df_appetite', 'df_appetite_change_last_year', 'df_ever_malnourished', 'df_ever_underweight', 'df_ever_overweight', 'df_weight_change_last_year', 'mp_dental_examination_frequency', 'mp_dental_brushing_frequency', 'mp_dental_treat_frequency', 'mp_dental_food_frequency', 'mp_dental_breath_freshener_frequency', 'mp_dental_procedure_undergone', 'mp_dental_cleaning', 'mp_dental_cleaning_months_ago', 'mp_dental_extraction', 'mp_dental_extraction_months_ago', 'mp_professional_grooming', 'mp_professional_grooming_frequency', 'mp_professional_grooming_shampoos_regular', 'mp_professional_grooming_shampoos_flea_or_tick_control', 'mp_professional_grooming_shampoos_medicated', 'mp_professional_grooming_shampoos_none', 'mp_professional_grooming_shampoos_unknown', 'mp_professional_grooming_shampoos_other', 'mp_professional_grooming_shampoos_other_description', 'mp_home_grooming', 'mp_home_grooming_frequency', 'mp_home_grooming_shampoos_regular', 'mp_home_grooming_shampoos_flea_or_tick_control', 'mp_home_grooming_shampoos_medicated', 'mp_home_grooming_shampoos_none', 'mp_home_grooming_shampoos_unknown', 'mp_home_grooming_shampoos_other', 'mp_home_grooming_shampoos_other_description', 'mp_flea_and_tick_treatment', 'mp_flea_and_tick_treatment_frequency', 'mp_flea_and_tick_treatment_topical', 'mp_flea_and_tick_treatment_oral', 'mp_flea_and_tick_treatment_dip', 'mp_flea_and_tick_treatment_collar', 'mp_flea_and_tick_treatment_other', 'mp_flea_and_tick_treatment_other_description', 'mp_heartworm_preventative', 'mp_heartworm_preventative_frequency', 'mp_heartworm_preventative_oral_chewable', 'mp_heartworm_preventative_oral_solution', 'mp_heartworm_preventative_topical', 'mp_heartworm_preventative_injectable', 'mp_heartworm_preventative_other', 'mp_heartworm_preventative_other_description', 'mp_vaccination_status', 'mp_recent_non_prescription_meds_antibiotic_ointment', 'mp_recent_non_prescription_meds_antihistamine', 'mp_recent_non_prescription_meds_anti_inflammatory', 'mp_recent_non_prescription_meds_ear_cleaner', 'mp_recent_non_prescription_meds_enzymes', 'mp_recent_non_prescription_meds_eye_lubricant', 'mp_recent_non_prescription_meds_joint_supplement', 'mp_recent_non_prescription_meds_upset_stomach', 'mp_recent_non_prescription_meds_omega3', 'mp_recent_non_prescription_meds_non_oil_skin', 'mp_recent_non_prescription_meds_probiotic', 'mp_recent_non_prescription_meds_acidify_supplement', 'mp_recent_non_prescription_meds_alkalinize_supplement', 'mp_recent_non_prescription_meds_vitamin', 'mp_recent_non_prescription_meds_other', 'mp_recent_non_prescription_meds_other_description', 'ss_vet_frequency', 'hs_general_health', 'hs_new_condition_diagnosed_recently', 'hs_new_condition_diagnosed_last_month', 'hs_chronic_condition_present', 'hs_chronic_condition_recently_changed_or_treated', 'hs_congenital_condition_present', 'hs_recent_hospitalization', 'hs_recent_hospitalization_reason_spay_or_neuter', 'hs_recent_hospitalization_reason_dentistry', 'hs_recent_hospitalization_reason_boarding', 'hs_recent_hospitalization_reason_other', 'hs_recent_hospitalization_reason_other_description', 'hs_health_conditions_eye', 'hs_health_conditions_ear', 'hs_health_conditions_oral', 'hs_health_conditions_skin', 'hs_health_conditions_cardiac', 'hs_health_conditions_respiratory', 'hs_health_conditions_gastrointestinal', 'hs_health_conditions_liver', 'hs_health_conditions_kidney', 'hs_health_conditions_reproductive', 'hs_health_conditions_orthopedic', 'hs_health_conditions_neurological', 'hs_health_conditions_endocrine', 'hs_health_conditions_hematologic', 'hs_health_conditions_immune', 'hs_health_conditions_infectious_disease', 'hs_health_conditions_toxin_consumption', 'hs_health_conditions_trauma', 'hs_health_conditions_cancer', 'hs_health_conditions_other', 'hs_alternative_care_none', 'hs_alternative_care_acupuncture', 'hs_alternative_care_herbal_medicine', 'hs_alternative_care_homeopathy', 'hs_alternative_care_chiropractic', 'hs_alternative_care_massage', 'hs_alternative_care_rehabilitation_therapy', 'hs_alternative_care_reiki', 'hs_alternative_care_traditional_chinese_medicine', 'hs_alternative_care_other', 'hs_alternative_care_other_description', 'hs_other_medical_info', 'fs_primary_care_veterinarian_exists', 'fs_primary_care_veterinarian_consent_share_vemr', 'fs_primary_care_veterinarian_can_provide_email', 'fs_primary_care_veterinarian_state', 'fs_future_studies_participation_likelihood', 'fs_phenotype_vs_lifespan_participation_likelihood', 'fs_genotype_vs_lifespan_participation_likelihood', 'fs_medically_slowed_aging_participation_likelihood']
    return column_order


# create a service object to handle all aspects of generating a primary key
@dataclass
class PrimaryKeyGenerator:
    table_name: str
    pk_name: str = field(init=False)
    firecloud: bool

    # this will calculate pk_name during init
    def __post_init__(self) -> None:
        # most tables should have "dog_id" as a key
        if self.table_name in {"hles_dog", "hles_cancer_condition", "hles_health_condition", "environment", "cslb",
                               "eols"}:
            self.pk_name = 'dog_id'
        # owner table is linked to hles_dog via "owner_id"
        elif self.table_name == 'hles_owner':
            self.pk_name = 'owner_id'
        elif self.table_name == 'sample':
            self.pk_name = 'sample_id'
        else:
            raise ValueError(f"Unrecognized table: {self.table_name}")

    def generate_entity_name(self) -> str:
        if self.firecloud:
            return f"{PRIMARY_KEY_PREFIX}:{self.table_name}_id"
        return self.pk_name

    def generate_primary_key(self, row: dict[str, str]) -> str:
        # normal processing of IDs - the original primary key is returned
        if not self.firecloud:
            return row.pop(self.pk_name)
        # generate ids for Firecloud compatibility
        # hles_health_condition: read + copy dog_id and hs_condition and hs_condition_is_congenital
        # concatenate and write out as entity_name, keep original PK
        if self.table_name == "hles_health_condition":
            # code to generate the value of primary key should be pulled out
            # grab the congenital flag and convert to int
            congenital_flag = row.get('hs_condition_is_congenital')
            try:
                congenital_flag = int(congenital_flag)  # type: ignore
            except TypeError:
                log.warning(f"Error, 'hs_condition_is_congenital' is not populated in {self.table_name}")
            return '-'.join(
                [str(component) for component in [row.get('dog_id'), row.get('hs_condition_type'), row.get('hs_condition'), congenital_flag]])
        # environment: read + copy dog_id and address fields to concatenate for generated uuid
        elif self.table_name == "environment":
            primary_key_fields = ['dog_id', 'address_1_or_2', 'address_month', 'address_year']
            return '-'.join([str(row.get(field)) for field in primary_key_fields])
        # cslb: read + copy dog_id and cslb_date to concatenate for generated uuid
        elif self.table_name == "cslb":
            primary_key_fields = ['dog_id', 'cslb_date']
            return '-'.join([str(row.get(field)) for field in primary_key_fields])
        # sample_id would match the entity primary key, popping the original column here to prevent duplicate column
        elif self.table_name in {"hles_owner", "sample"}:
            return row.pop(self.pk_name)
        # all other tables: return the original PK to be duplicated to the new column
        else:
            return str(row.get(self.pk_name))


def _open_output_location(output_location: str, gcs: GCSFileSystem) -> Union[Any, io.TextIOWrapper]:
    if output_location.startswith("gs://"):
        return gcs.open(output_location, 'w')

    target_dir = os.path.dirname(output_location)
    if not os.path.isdir(target_dir):
        os.mkdir(target_dir)
    return open(output_location, 'w')


def convert_to_tsv(input_dir: str, output_dir: str, firecloud: bool,
                   survey_types: Optional[list[DapSurveyType]] = None) -> None:
    # Process the known (hardcoded) tables
    if survey_types is None:
        table_names = DEFAULT_TABLE_NAMES
    else:
        table_names = []
        for survey_type in survey_types:
            table_names = table_names + TABLES_BY_SURVEY[survey_type]

    for table_name in table_names:
        log.info(f"PROCESSING {table_name}")
        primary_key_gen = PrimaryKeyGenerator(table_name, firecloud)
        # set to hold all columns for this table, list to hold all the rows
        ordered_columns = []
        row_list = []
        # generated PK column headers for Firecloud compatibility
        entity_name = primary_key_gen.generate_entity_name()

        # get the default column order for the respective table
        ordered_columns = getColumnOrder(table_name)

        # read json data
        gcs = GCSFileSystem()
        for path in gcs.ls(os.path.join(input_dir, table_name)):
            log.info(f"...Opening {path}")

            with gcs.open(path, 'r') as json_file:
                print(path, json_file)
                for line in json_file:
                    row = json.loads(line)

                    if not row:
                        raise RuntimeError(f'Encountered invalid JSON "{line}", aborting.')

                    row[entity_name] = primary_key_gen.generate_primary_key(row)

                    row_list.append(row)

        # make sure pk is the first column (Firecloud req.)
        if firecloud:
            # retain the "dog_id" column for tables where it is duplicated to an entity column
            if primary_key_gen.pk_name == "dog_id":
                ordered_columns = [entity_name] + ordered_columns
                log.debug(f"Final column order: {ordered_columns}")
            # do not include the original pk in the columns tables with a different PK (hles_owner, sample)
            else:
                ordered_columns.remove(primary_key_gen.pk_name)
                ordered_columns = [entity_name] + ordered_columns
                log.debug(f"Final column order: {ordered_columns}")

        # provide some stats
        col_count = len(ordered_columns)
        log.info(f"...{table_name} contains {len(row_list)} rows and {col_count} columns")
        # output to tsv
        # 512 column max limit per request (upload to workspace)
        if col_count > TERRA_COLUMN_LIMIT:
            # calculate chunks needed - each table requires the PK
            total_col_count = ceil(col_count / TERRA_COLUMN_LIMIT) + col_count - 1
            chunks = ceil(total_col_count / TERRA_COLUMN_LIMIT)
            log.info(f"...Splitting {table_name} into {chunks} files")
            log.info(f"...{len(ordered_columns)} cols in init list")
            # FOR EACH SPLIT
            for chunk in range(1, chunks + 1):
                # Incremented outfile name
                output_location = os.path.join(output_dir, f'{table_name}_{chunk}.tsv')
                log.info(f"...Processing Split #{chunk} to {output_location}")
                split_column_set = set()
                # add 511 columns
                for _ in range(TERRA_COLUMN_LIMIT - 1):
                    try:
                        col = ordered_columns.pop()
                    except KeyError:
                        # KeyError is raised when we're out of columns to pop
                        break
                    split_column_set.add(col)
                    log.debug(
                        f"......adding {col} to split_column_set ({len(split_column_set)}) ...{len(ordered_columns)} columns left")
                split_column_list = [entity_name] + sorted(list(split_column_set))
                log.info(f"......Split #{chunk} now contains {len(split_column_list)} columns")
                log.debug(f"cols in split: {len(split_column_list)}")
                log.debug(f"cols left to split: {len(ordered_columns)}")

                def clean_up_string_whitespace(val: str) -> str:
                    try:
                        return val.replace('\r\n', ' ').strip()
                    except AttributeError:
                        return val

                # iterate through every row slicing out the values for the columns in this split
                split_row_dict_list = [
                    {
                        col: clean_up_string_whitespace(row[col])
                        for col in split_column_list
                        if col in row
                    }
                    for row in row_list
                ]

                # output to tsv
                with _open_output_location(output_location, gcs) as output_file:
                    dw = csv.DictWriter(output_file, split_column_list, delimiter='\t')
                    dw.writeheader()
                    dw.writerows(split_row_dict_list)

                log.info(f"......{table_name} Split #{chunk} was successfully written to {output_location}")
        else:
            # this branch is executed for any tables with 512 cols or less
            log.info(f"...No need to split files for {table_name}")
            output_location = os.path.join(output_dir, f'{table_name}.tsv')

            with _open_output_location(output_location, gcs) as output_file:
                log.info(f"...Writing {table_name} to  path {output_location}")
                dw = csv.DictWriter(output_file, ordered_columns, delimiter='\t')
                dw.writeheader()
                dw.writerows(row_list)

            log.info(f"...{table_name} was successfully written to {output_location}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Convert the records in a specified bucket prefix to a Terra-compatible TSV")
    parser.add_argument('input_dir', metavar='I', help='The bucket prefix to read records from')
    parser.add_argument('output_dir', metavar='O', help='The local directory to write the resulting TSVs to')
    parser.add_argument('survey_types', nargs='*',
                        help="One or more survey types to process into TSVs. Processes all tables if none specified.")
    parser.add_argument('--firecloud', action='store_true',
                        help="Use logic to generate primary keys for Terra upload via Firecloud")
    parser.add_argument('--debug', action='store_true', help="Write additional logs for debugging")

    parsed = parser.parse_args()

    log_level = logging.DEBUG if parsed.debug else logging.INFO
    logging.basicConfig(level=log_level)

    convert_to_tsv(parsed.input_dir, parsed.output_dir, parsed.firecloud, parsed.survey_types)
