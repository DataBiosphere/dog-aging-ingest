import csv
import unittest

CSLB_PATH = 'tsv_output/cslb.tsv'
HLES_DOG_PATH = 'tsv_output/hles_dog.tsv'

class ReleaseValidationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._cslb_data = cls._parse_file(CSLB_PATH)
        cls._hles_dog_data = cls._parse_file(HLES_DOG_PATH)

    @classmethod
    def _parse_file(cls, path):
        with open(path) as f:
            return [row for row in csv.DictReader(f, delimiter='\t')]

    @classmethod
    def find_affected_row(cls, data, dog_id):
        return next(row for row in data if row['entity:dog_id_id'] == dog_id)

    def test_cslb_date(self):
        for row in self._cslb_data:
            self.assertNotIn('2021', row['cslb_date'], msg=f"Row ID {row['entity:cslb_id']} has a 2021 cslb_date")
            self.assertIn('2020', row['cslb_date'],
                          msg=f"Row ID {row['entity:cslb_id']} does not have a 2020 cslb_date")

    def test_spay_or_neuter_age(self):
        for row in self._hles_dog_data:
            self.assertIn('dd_spay_or_neuter_age', row)
            self.assertIn(row['dd_spay_or_neuter_age'], {'', '1', '2', '3', '4', '5', '6', '7', '8', '99'})

    def test_vet_can_provide_email(self):
        for row in self._hles_dog_data:
            self.assertIn('fs_primary_care_veterinarian_can_provide_email', row)
            self.assertIn(row['fs_primary_care_veterinarian_can_provide_email'], {'1', '7', '8', ''})

    def test_zip_nbr(self):
        for row in self._hles_dog_data:
            self.assertIn('de_past_residence_zip_count', row)
            cnt = int(row['de_past_residence_zip_count'])
            self.assertTrue(cnt >= 0)

    def test_st_label_not_present(self):
        for row in self._hles_dog_data:
            self.assertNotIn('st_batch_label', row)
        for row in self._hles_dog_data:
            self.assertNotIn('st_batch_label', row)

    def test_daily_bone_meal_supplements_sane(self):
        for row in self._hles_dog_data:
            self.assertIn('df_daily_supplements_bone_meal', row)
            self.assertIn(row['df_daily_supplements_bone_meal'], {'', '0', '1', '2'})

    def test_infreq_bone_meal_supplements_sane(self):
        for row in self._hles_dog_data:
            self.assertIn('df_infrequent_supplements_bone_meal', row)
            self.assertIn(row['df_infrequent_supplements_bone_meal'], {'', '0', '1', '2'})

    def test_de_interacts_with_neighborhood_animals_with_owner(self):
        for row in self._hles_dog_data:
            self.assertIn('de_interacts_with_neighborhood_animals_with_owner', row)
            self.assertIn(row['de_interacts_with_neighborhood_animals_with_owner'], {'True', 'False', ''})

    def test_de_interacts_with_neighborhood_humans_with_owner(self):
        for row in self._hles_dog_data:
            self.assertIn('de_interacts_with_neighborhood_humans_with_owner', row)
            self.assertIn(row['de_interacts_with_neighborhood_humans_with_owner'], {'True', 'False', ''})

    def test_dd_acquired_st(self):
        found_affected_rows = False
        for row in self._hles_dog_data:
            if row['entity:dog_id_id'] in ('18069', '32705', '40519', '63438', '89055'):
                found_affected_rows = True
                self.assertNotIn(row['dd_acquired_state'], {'', 'NA'})

        self.assertTrue(found_affected_rows)

    def test_dd_activities_obedience(self):
        affected_obedience_row = self.find_affected_row(self._hles_dog_data, '76099')
        self.assertIn('dd_activities_obedience', affected_obedience_row)
        self.assertEqual(affected_obedience_row['dd_activities_obedience'], '')

    def test_dd_activities_agility(self):
        affected_agility_row = self.find_affected_row(self._hles_dog_data, '34888')
        self.assertIn('dd_activities_agility', affected_agility_row)
        self.assertEqual(affected_agility_row['dd_activities_agility'], '')

    def test_dd_activities_breeding(self):
        affected_breeding_row = self.find_affected_row(self._hles_dog_data, '66316')
        self.assertIn('dd_activities_breeding', affected_breeding_row)
        self.assertEqual(affected_breeding_row['dd_activities_breeding'], '')

    def test_dd_activities_hunting(self):
        affected_hunting_row = self.find_affected_row(self._hles_dog_data, '19971')
        self.assertIn('dd_activities_hunting', affected_hunting_row)
        self.assertEqual(affected_hunting_row['dd_activities_hunting'], '')

    def test_dd_activities_field_trials(self):
        affected_field_trials_row = self.find_affected_row(self._hles_dog_data, '13359')
        self.assertIn('dd_activities_field_trials', affected_field_trials_row)
        self.assertEqual(affected_field_trials_row['dd_activities_field_trials'], '')

    def test_dd_activities_working(self):
        affected_working_row = self.find_affected_row(self._hles_dog_data, '82623')
        self.assertIn('dd_activities_working', affected_working_row)
        self.assertEqual(affected_working_row['dd_activities_working'], '')

    def test_st_portal_account_creation_date(self):
        for row in self._hles_dog_data:
            self.assertIn('st_portal_account_creation_date', row)
            self.assertTrue(bool(row['st_portal_account_creation_date']))

    def test_past_residence_country(self):
        self.assertTrue(any(row['de_past_residence_country1_text'] for row in self._hles_dog_data))
        self.assertTrue(any(row['de_past_residence_country2_text'] for row in self._hles_dog_data))

    def test_alt_care_none(self):
        self.assertTrue(any(row['hs_alternative_care_none'] for row in self._hles_dog_data))

    def test_removed_past_countries(self):
        self.assertTrue(all('de_past_residence_country3_text' not in row for row in self._hles_dog_data))

    def test_combined_past_countries(self):
        affected_country_row = self.find_affected_row(self._hles_dog_data, '688')
        self.assertIn('de_past_residence_country1', affected_country_row)
        self.assertEqual(affected_country_row['de_past_residence_country1'], 'BS')

if __name__ == '__main__':
    unittest.main()
