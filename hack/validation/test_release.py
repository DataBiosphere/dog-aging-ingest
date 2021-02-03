import csv
import pytest

CSLB_PATH = 'tsv_output/cslb.tsv'
HLES_DOG1_PATH = 'tsv_output/hles_dog_1.tsv'
HLES_DOG2_PATH = 'tsv_output/hles_dog_2.tsv'


@pytest.fixture(scope='session')
def cslb_data():
    return parse_file(CSLB_PATH)


@pytest.fixture(scope='session')
def hles_dog1_data():
    return parse_file(HLES_DOG1_PATH)


@pytest.fixture(scope='session')
def hles_dog2_data():
    return parse_file(HLES_DOG2_PATH)


def parse_file(path):
    with open(path) as f:
        return [row for row in csv.DictReader(f, delimiter='\t')]


def test_cslb_date(cslb_data):
    for row in cslb_data:
        assert '2021' not in row['cslb_date']
        assert '2020' in row['cslb_date']


def test_spay_or_neuter_age(hles_dog2_data):
    for row in hles_dog2_data:
        assert 'dd_spay_or_neuter_age' in row
        assert row['dd_spay_or_neuter_age'] in {'', '5', '99', '2', '4', '7', '6', '8', '3', '1'}


def test_vet_can_provide_email(hles_dog2_data):
    for row in hles_dog2_data:
        assert 'fs_primary_care_veterinarian_can_provide_email' in row
        assert row['fs_primary_care_veterinarian_can_provide_email'] in {'1', '7', '8', ''}


def test_zip_nbr(hles_dog1_data):
    for row in hles_dog1_data:
        assert 'de_past_residence_zip_count' in row
        cnt = int(row['de_past_residence_zip_count'])
        assert (cnt >= 0)


def test_st_label_not_present(hles_dog1_data, hles_dog2_data):
    for row in hles_dog1_data:
        assert 'st_batch_label' not in row
    for row in hles_dog2_data:
        assert 'st_batch_label' not in row


def test_daily_bone_meal_supplements_sane(hles_dog1_data):
    for row in hles_dog1_data:
        assert 'df_daily_supplements_bone_meal' in row
        assert row['df_daily_supplements_bone_meal'] in {'', '0', '1', '2'}


def test_infreq_bone_meal_supplements_sane(hles_dog1_data):
    for row in hles_dog1_data:
        assert 'df_infrequent_supplements_bone_meal' in row
        assert row['df_infrequent_supplements_bone_meal'] in {'', '0', '1', '2'}


def test_de_interacts_with_neighborhood_animals_with_owner(hles_dog1_data):
    for row in hles_dog1_data:
        assert 'de_interacts_with_neighborhood_animals_with_owner' in row
        assert row['de_interacts_with_neighborhood_animals_with_owner'] in {'True', 'False', ''}


def test_de_interacts_with_neighborhood_humans_with_owner(hles_dog2_data):
    for row in hles_dog2_data:
        assert 'de_interacts_with_neighborhood_humans_with_owner' in row
        assert row['de_interacts_with_neighborhood_humans_with_owner'] in {'True', 'False', ''}


def test_dd_acquired_st(hles_dog1_data):
    found_affected_row = False
    for row in hles_dog1_data:
        if row['entity:dog_id_id'] in ('18069', '32705', '40519', '63438', '89055'):
            found_affected_row = True
            assert row['dd_acquired_state'] not in {'', 'NA'}

    assert found_affected_row is True


def test_dd_activities_obedience(hles_dog1_data):
    found_affected_row = False
    for row in hles_dog1_data:
        if row['entity:dog_id_id'] in {'76099'}:
            assert 'dd_activities_obedience' in row
            assert row['dd_activities_obedience'] == ''
            found_affected_row = True
    assert found_affected_row is True


def test_dd_activities_agility(hles_dog1_data):
    found_affected_row = False
    for row in hles_dog1_data:
        if row['entity:dog_id_id'] in {'34888'}:
            assert 'dd_activities_agility' in row
            assert row['dd_activities_agility'] == ''
            found_affected_row = True
    assert found_affected_row is True


def test_dd_activities_breeding(hles_dog1_data):
    found_affected_row = False
    for row in hles_dog1_data:
        if row['entity:dog_id_id'] in {'66316'}:
            assert 'dd_activities_breeding' in row
            assert row['dd_activities_breeding'] == ''
            found_affected_row = True
    assert found_affected_row is True


def test_dd_activities_breeding(hles_dog1_data):
    found_affected_row = False
    for row in hles_dog1_data:
        if row['entity:dog_id_id'] in {'19971'}:
            assert 'dd_activities_hunting' in row
            assert row['dd_activities_hunting'] == ''
            found_affected_row = True
    assert found_affected_row is True


def test_dd_activities_field_trials(hles_dog1_data):
    found_affected_row = False
    for row in hles_dog1_data:
        if row['entity:dog_id_id'] in {'13359'}:
            assert 'dd_activities_field_trials' in row
            assert row['dd_activities_field_trials'] == ''
            found_affected_row = True
    assert found_affected_row is True


def test_dd_activities_working(hles_dog1_data):
    found_affected_row = False
    for row in hles_dog1_data:
        if row['entity:dog_id_id'] in {'82623'}:
            assert 'dd_activities_working' in row
            assert row['dd_activities_working'] == ''
            found_affected_row = True
    assert found_affected_row is True


def test_st_portal_account_creation_date(hles_dog1_data):
    for row in hles_dog1_data:
        assert 'st_portal_account_creation_date' in row
        assert len(row['st_portal_account_creation_date']) > 0


def test_past_residence_country(hles_dog2_data, hles_dog1_data):
    past_residence_country1_vals = set()
    past_residence_country2_vals = set()

    for row in hles_dog2_data:
        assert 'de_past_residence_country1_text' in row
        if row['de_past_residence_country1_text']:
            past_residence_country1_vals.add(row['de_past_residence_country1_text'])

    for row in hles_dog1_data:
        assert 'de_past_residence_country2_text' in row
        if row['de_past_residence_country2_text']:
            past_residence_country2_vals.add(row['de_past_residence_country2_text'])

    assert len(past_residence_country1_vals) > 0
    assert len(past_residence_country2_vals) > 0
