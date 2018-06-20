import unittest
from biothings.utils.keylookup_api import KeyLookupMyChemInfo
from biothings.utils.keylookup_api import KeyLookupMyGeneInfo


class TestKeyLookupAPI(unittest.TestCase):

    def test_mycheminfo(self):
        """
        Test of KeyLookupMyChemInfo
        :return:
        """

        def _MyChemInfoSingleDoc(input_type, output_types, question, answer):
            @KeyLookupMyChemInfo(input_type, output_types)
            def load_document(doc_lst):
                for d in doc_lst:
                    yield d
            doc_lst = [{'_id': question}]
            res_lst = load_document(doc_lst)
            res = next(res_lst)
            self.assertEqual(res['_id'], answer)

        # Examples - paracetamol (acetaminophen)
        _MyChemInfoSingleDoc('inchikey', ['inchikey'], 'RZVAJINKPMORJF-UHFFFAOYSA-N', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('chebi', ['inchikey'], 'CHEBI:46195', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('unii', ['inchikey'], '362O9ITL9D', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('drugbank', ['inchikey'], 'DB00316', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('chembl', ['inchikey'], 'CHEMBL112', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('pubchem', ['inchikey'], 'CID1983', 'RZVAJINKPMORJF-UHFFFAOYSA-N')

        # Other examples
        _MyChemInfoSingleDoc('chebi', ['inchikey'], 'CHEBI:63599', 'GIUYCYHIANZCFB-FJFJXFQQSA-N')
        _MyChemInfoSingleDoc('inchikey', ['pubchem'], 'ATBDZSAENDYQDW-UHFFFAOYSA-N', 'CID4080429')
        _MyChemInfoSingleDoc('inchikey', ['unii'], 'ATBDZSAENDYQDW-UHFFFAOYSA-N', '18MXK3D6DB')

    def test_mygeneinfo(self):
        """
        Test of KeyLookupMyGeneInfo
        :return:
        """

        def _MyGeneInfoSingleDoc(input_type, output_types, question, answer):
            @KeyLookupMyGeneInfo(input_type, output_types)
            def load_document(doc_lst):
                for d in doc_lst:
                    yield d
            doc_lst = [{'_id': question}]
            res_lst = load_document(doc_lst)
            res = next(res_lst)
            self.assertEqual(res['_id'], answer)

        _MyGeneInfoSingleDoc('ensembl', ['symbol'], 'ENSG00000123374', 'CDK2')
        _MyGeneInfoSingleDoc('entrezgene', ['symbol'], '1017', 'CDK2')

        # TODO:  uniprot.Swiss-Prot doesn't with query_many
        # _MyGeneInfoSingleDoc('uniprot', ['symbol'], 'P24941', 'CDK2')

        # Test multiple output types (uniprot will be skipped)
        _MyGeneInfoSingleDoc('entrezgene', ['uniprot', 'ensembl', 'symbol'], '105864946', 'ENSMICG00000026391')

    def test_mygene_one2many(self):
        """
        Test the one-to-many relationship for key conversion

        :return:
        """

        doc_lst = [{'_id': 'CDK2'}]
        @KeyLookupMyGeneInfo('symbol', ['ensembl'], skip_on_failure=True)
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res_cnt = sum(1 for _ in res_lst)
        # assert that at least 5 elements are returned
        self.assertGreater(res_cnt, 5)

    def test_batch_queries(self):
        """
        Test converting a long-ish list of entrezgenes to symbols.  The
        purpose of this test is to exercise the query_many behavior of
        the class which will break the list into batches.
        :return:
        """

        # Build up document list
        input = [
            51300,
            54958,
            57829,
            100526772,
            6836,
            84910,
            644672,
            643382,
            348013,
            2707400000 # broken on purpose
        ]
        doc_lst = []
        for e in input:
            doc_lst.append({'_id': e})

        answers = [
            'TIMMDC1',
            'TMEM160',
            'ZP4',
            'TMEM110-MUSTN1',
            'SURF4',
            'TMEM87B',
            'CLDN25',
            'TMEM253',
            'TMEM255B',
            # The last key was not converted
            2707400000
        ]

        # Test a list being passed with 10 documents
        @KeyLookupMyGeneInfo('entrezgene', ['symbol'])
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res_cnt = 0
        for res in res_lst:
            res_cnt += 1
            if not res['_id'] in answers:
                print(res['_id'])
            self.assertTrue(res['_id'] in answers)
        self.assertEqual(res_cnt, 10)

    def test_strangecases(self):

        doc_lst = [{'_id': 'CDK2'}]

        # with self.assertRaises(ValueError):
        with self.assertRaises(ValueError):
            @KeyLookupMyGeneInfo('entrezgene', ['undefined'])
            def load_document(data_folder):
                for d in doc_lst:
                    yield d

        # Non-string input-type
        with self.assertRaises(ValueError):
            @KeyLookupMyGeneInfo(None, ['undefined'])
            def load_document(data_folder):
                for d in doc_lst:
                    yield d

        # Non-list output-type
        with self.assertRaises(ValueError):
            @KeyLookupMyGeneInfo('entrezgene', 'symbol')
            def load_document(data_folder):
                for d in doc_lst:
                    yield d

        # output-type with a non-string
        with self.assertRaises(ValueError):
            @KeyLookupMyGeneInfo('entrezgene', [None])
            def load_document(data_folder):
                for d in doc_lst:
                    yield d

    def test_invalid_record(self):
        """
        Test an invalid record in the document set.
        :return:
        """

        doc_lst = [{'_id': 'CID1983'}, {'_id': None}, {'id': 'CID1983'}]
        @KeyLookupMyChemInfo('pubchem', ['inchikey'], skip_on_failure=True)
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res_cnt = sum(1 for _ in res_lst)
        self.assertEqual(res_cnt, 1)

    def test_inchi_to_inchikey(self):
        """
        Inchi strings are long, and contain characters that other fields often
        do not.  It is a good test case to convert a couple of them to inchikeys.
        This is used in several mychem.info parsers.
        :return:
        """
        inchi1 = "InChI=1S/C19H25N3O7S/c1-19(2)14(17(26)27)22-15(30-19)13(18(28)29-9-11(20)16(24)25)21-12(23)8-10-6-4-3-5-7-10/h3-7,11,13-15,22H,8-9,20H2,1-2H3,(H,21,23)(H,24,25)(H,26,27)/p-2/t11-,13-,14-,15+/m0/s1"
        inchi2 = "InChI=1S/C8H9NO2/c1-6(10)9-7-2-4-8(11)5-3-7/h2-5,11H,1H3,(H,9,10)"

        doc_lst = [
            {'_id': inchi1},
            {'_id': inchi2},
        ]

        @KeyLookupMyChemInfo('dinchi', ['dinchikey'])
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document(doc_lst)
        res = next(res_lst)
        self.assertEqual(res['_id'], 'USNINKBPBVKHHZ-CYUUQNCZSA-L')
        res = next(res_lst)
        self.assertEqual(res['_id'], 'RZVAJINKPMORJF-UHFFFAOYSA-N')

    def test_input_source_fields(self):
        """
        Test input source field options.  These are complicated tests with input source field
        of varying depth and complexity.  Multiple documents are converted.
        Conversion to InchiKey is performed.
        :return:
        """

        doc_lst = [
            {
                '_id': 'test1_acetomenaphin',
                'pharmgkb': {
                    'inchi': 'InChI=1S/C8H9NO2/c1-6(10)9-7-2-4-8(11)5-3-7/h2-5,11H,1H3,(H,9,10)'
                }
            },
            {
                '_id': 'test2_statin',
                'pharmgkb': {
                    'xref': {
                        'drugbank_id': 'DB01076'
                    }
                }
            },
            {
                '_id': 'test3_LithiumCarbonate',
                'pharmgkb': {
                    'xref': {
                        'pubchem_cid': 'CID11125'
                    }
                }
            },
            {
                '_id': 'test4_IBUPROFEN',
                'pharmgkb': {
                    'xref': {
                        'chembl_id': 'CHEMBL521'
                    }
                }
            }
        ]

        @KeyLookupMyChemInfo([('dinchi', 'pharmgkb.inchi'), ('drugbank', 'pharmgkb.xref.drugbank_id'), ('pubchem', 'pharmgkb.xref.pubchem_cid'),('chembl', 'pharmgkb.xref.chembl_id')], ['dinchikey', 'cinchikey', 'pinchikey'])
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        r = next(res_lst)
        self.assertEqual(r['_id'], 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        r = next(res_lst)
        self.assertEqual(r['_id'], 'XUKUURHRXDUEBC-KAYWLYCHSA-N')
        r = next(res_lst)
        self.assertEqual(r['_id'], 'XGZVUEUWXADBQD-UHFFFAOYSA-L')
        r = next(res_lst)
        self.assertEqual(r['_id'], 'HEFNNWSXXWATRW-UHFFFAOYSA-N')

    @unittest.skip("Test is too long for routine use")
    def test_long_doc_lst(self):
        """
        Test a document list containing 10000 entries.  Verify that the correct
        number of documents are returned.

        Unfortunately in this test there are a large number of duplicates, but the
        correct number of documents are returned.
        :return:
        """
        # Build up document list
        input = 51300
        doc_lst = []
        for i in range(10000):
            doc_lst.append({'_id': input})

        answers = [
            'TIMMDC1'
        ]

        # Test a list being passed with 10 documents
        @KeyLookupMyGeneInfo('entrezgene', ['symbol'])
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res_cnt = 0
        for res in res_lst:
            res_cnt += 1
            self.assertTrue(res['_id'] in answers)
        self.assertEqual(res_cnt, 10000)