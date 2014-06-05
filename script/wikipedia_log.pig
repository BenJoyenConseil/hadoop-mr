REGISTER wikipedia.jar;
DEFINE CustomInput wikipedia.pig.FileNameTextLoadFunc('20071209');
DEFINE SIM wikipedia.pig.SimilarityFunc();

records = LOAD 'wikipedia' USING CustomInput AS (filename: chararray, lang: chararray, page: chararray, visit: long);

filterbylang = FILTER records BY lang == 'fr' OR lang == 'en' OR lang == 'de';

restrictions = LOAD 'page_names_to_skip.txt' USING TextLoader() AS (page: chararray);

join1 = JOIN filterbylang BY page LEFT OUTER, restrictions BY page;

filter2 = FILTER join1 BY SIM(filterbylang::page, restrictions::page) == false;

groupbypage = COGROUP filter2 BY (filterbylang::page, filterbylang::lang);

sumrecords = FOREACH groupbypage GENERATE group.lang, group.page, SUM(filter2.filterbylang::visit) AS visit_sum;

group1 = GROUP sumrecords BY lang;

top20 = FOREACH group1 {
	sorted = ORDER sumrecords BY visit_sum DESC;
	top1 = LIMIT sorted 20;
	GENERATE group, flatten(top1);
};


dump top20;
