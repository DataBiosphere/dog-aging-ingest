import csv
import unittest

CSLB_PATH = 'tsv_output/cslb.tsv'
HLES_DOG1_PATH = 'tsv_output/hles_dog_1.tsv'
HLES_DOG2_PATH = 'tsv_output/hles_dog_2.tsv'


class ReleaseValidationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._cslb_data = cls._parse_file(CSLB_PATH)
        cls._hles_dog1_data = cls._parse_file(HLES_DOG1_PATH)
        cls._hles_dog2_data = cls._parse_file(HLES_DOG2_PATH)

    @classmethod
    def _parse_file(cls, path):
        with open(path) as f:
            return [row for row in csv.DictReader(f, delimiter='\t')]

    @classmethod
    def find_affected_row(cls, data, dog_id):
        return next(row for row in data if row['entity:dog_id_id'] == dog_id)

    def test_cslb_date(self):
        for row in self._cslb_data:
            assert '2021' not in row['cslb_date']
            assert '2020' in row['cslb_date']

    def test_spay_or_neuter_age(self):
        for row in self._hles_dog2_data:
            assert 'dd_spay_or_neuter_age' in row
            assert row['dd_spay_or_neuter_age'] in {'', '1', '2', '3', '4', '5', '6', '7', '8', '99'}

    def test_vet_can_provide_email(self):
        for row in self._hles_dog2_data:
            assert 'fs_primary_care_veterinarian_can_provide_email' in row
            assert row['fs_primary_care_veterinarian_can_provide_email'] in {'1', '7', '8', ''}

    def test_zip_nbr(self):
        for row in self._hles_dog1_data:
            assert 'de_past_residence_zip_count' in row
            cnt = int(row['de_past_residence_zip_count'])
            assert (cnt >= 0)

    def test_st_label_not_present(self):
        for row in self._hles_dog1_data:
            assert 'st_batch_label' not in row
        for row in self._hles_dog2_data:
            assert 'st_batch_label' not in row

    def test_daily_bone_meal_supplements_sane(self):
        for row in self._hles_dog1_data:
            assert 'df_daily_supplements_bone_meal' in row
            assert row['df_daily_supplements_bone_meal'] in {'', '0', '1', '2'}

    def test_infreq_bone_meal_supplements_sane(self):
        for row in self._hles_dog1_data:
            assert 'df_infrequent_supplements_bone_meal' in row
            assert row['df_infrequent_supplements_bone_meal'] in {'', '0', '1', '2'}

    def test_de_interacts_with_neighborhood_animals_with_owner(self):
        for row in self._hles_dog1_data:
            assert 'de_interacts_with_neighborhood_animals_with_owner' in row
            assert row['de_interacts_with_neighborhood_animals_with_owner'] in {'True', 'False', ''}

    def test_de_interacts_with_neighborhood_humans_with_owner(self):
        for row in self._hles_dog2_data:
            assert 'de_interacts_with_neighborhood_humans_with_owner' in row
            assert row['de_interacts_with_neighborhood_humans_with_owner'] in {'True', 'False', ''}

    def test_dd_acquired_st(self):
        found_affected_row = False
        for row in self._hles_dog1_data:
            if row['entity:dog_id_id'] in ('18069', '32705', '40519', '63438', '89055'):
                found_affected_row = True
                assert row['dd_acquired_state'] not in {'', 'NA'}

        assert found_affected_row

    def test_dd_activities_obedience(self):
        affected_obedience_row = self.find_affected_row(self._hles_dog1_data, '76099')
        assert 'dd_activities_obedience' in affected_obedience_row
        assert affected_obedience_row['dd_activities_obedience'] == ''

    def test_dd_activities_agility(self):
        affected_agility_row = self.find_affected_row(self._hles_dog1_data, '34888')
        assert 'dd_activities_agility' in affected_agility_row
        assert affected_agility_row['dd_activities_agility'] == ''

    def test_dd_activities_breeding(self):
        affected_breeding_row = self.find_affected_row(self._hles_dog1_data, '66316')
        self.assertIn('dd_activities_breeding', affected_breeding_row)
        self.assertEqual(affected_breeding_row['dd_activities_breeding'], '')

    def test_dd_activities_hunting(self):
        affected_hunting_row = self.find_affected_row(self._hles_dog1_data, '19971')
        self.assertIn('dd_activities_hunting', affected_hunting_row)
        self.assertEqual(affected_hunting_row['dd_activities_hunting'], '')

    def test_dd_activities_field_trials(self):
        affected_field_trials_row = self.find_affected_row(self._hles_dog1_data, '13359')
        self.assertIn('dd_activities_field_trials', affected_field_trials_row)
        self.assertEqual(affected_field_trials_row['dd_activities_field_trials'], '')

    def test_dd_activities_working(self):
        affected_working_row = self.find_affected_row(self._hles_dog1_data, '82623')
        self.assertIn('dd_activities_working', affected_working_row)
        self.assertEqual(affected_working_row['dd_activities_working'], '')

    def test_st_portal_account_creation_date(self):
        for row in self._hles_dog1_data:
            assert 'st_portal_account_creation_date' in row
            assert bool(row['st_portal_account_creation_date'])

    def test_past_residence_country(self):
        assert any(row['de_past_residence_country1_text'] for row in self._hles_dog2_data)
        assert any(row['de_past_residence_country2_text'] for row in self._hles_dog1_data)


if __name__ == '__main__':
    unittest.main()
