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
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            rows.append(row)

    return rows


def test_cslb_date(cslb_data):
    for row in cslb_data:
        assert '2021' not in row['cslb_date']
        assert '2020' in row['cslb_date']


def test_spay_or_neuter_age(hles_dog2_data):
    for row in hles_dog2_data:
        assert 'dd_spay_or_neuter_age' in row
        assert row['dd_spay_or_neuter_age'] in {'', '5', '99', '2', '4', '7', '6', '8', '3', '1'}


def test_vet_can_provide_email(hles_dog1_data):
    data_values = set()
    for row in hles_dog1_data:
        assert 'fs_primary_care_veterinarian_can_provide_email' in row
        data_values.add(row['fs_primary_care_veterinarian_can_provide_email'])

    assert data_values == {'1', '7', '8', ''}


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


def test_infreq_bone_meal_supplements_sane(hles_dog2_data):
    for row in hles_dog2_data:
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


def test_dd_acquired_st(hles_dog2_data):
    found_affected_row = False
    for row in hles_dog2_data:
        if row['entity:dog_id_id'] in ('18069', '32705', '40519', '63438', '89055'):
            found_affected_row = True
            assert row['dd_acquired_state'] not in {'', 'NA'}

    assert found_affected_row is True


def test_dd_activities_obedience(hles_dog1_data):
    found_affected_row = False
    for row in hles_dog1_data:
        if row['entity:dog_id_id'] in {'76099'}:
            assert row['dd_activities_obedience'] == '2'
            found_affected_row = True
    assert found_affected_row is True


def test_dd_activities_agility(hles_dog2_data):
    found_affected_row = False
    for row in hles_dog2_data:
        if row['entity:dog_id_id'] in {'34888'}:
            assert row['dd_activities_agility'] == '2'
            found_affected_row = True
    assert found_affected_row is True
