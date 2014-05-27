package wikipedia;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LogMapTest{
	
	MapDriver<Text, Text, CustomKey, LongWritable> mapDriver;
	ReduceDriver<CustomKey, LongWritable, CustomKey, LongWritable> reduceDriver;
	
	@Mock
	Context mapContext;
	
	@Before
	public void setup(){
		mapDriver = MapDriver.newMapDriver(new LogMapper());
		reduceDriver = ReduceDriver.newReduceDriver(new LogReducer());
        mapDriver.addCacheFile("languages_selection.txt");
        mapDriver.addCacheFile("page_names_to_skip.txt");
	}

	@Test
	public void map_ShouldRetrieveTopTenViewed_WikipediaPage() throws IOException{
		// Given
		String keyInStr = "pagecounts-20140501-000000";
		String valueInStr = "fr Tir_aux_Jeux_olympiques 14812 24343232";
		Text keyIn = new Text(keyInStr);
		Text valueIn = new Text(valueInStr);
		CustomKey keyOut = new CustomKey();
		keyOut.setLang("fr");
		keyOut.setMonth(5);
		keyOut.setYear(2014);
        keyOut.setDay(1);
        keyOut.setCount(14812);
		keyOut.setPageName("tir_aux_jeux_olympiques");
		LongWritable valueOut = new LongWritable();
		valueOut.set(14812L);
		mapDriver.withInput(keyIn, valueIn);
		mapDriver.withOutput(keyOut, valueOut);
		
		// When
		mapDriver.runTest();
		
		// Then
	}
	
	@Test
	public void isRecordLangSelected_shouldReturnTrue_WhenLangMatchTheConf(){
		// Given
		List list = new ArrayList<String>();
		list.add("en");
		list.add("fr");
		LogMapper logMapper = ((LogMapper)mapDriver.getMapper());
		logMapper.langagesSelection = list;
		CustomKey key = new CustomKey();
		key.setLang("fr");
		
		// Then
		boolean result = logMapper.isRecordLangSelected(key);
		
		// When
		Assert.assertThat(result, is(equalTo(true)));
	}
	
	@Test
	public void isRecordLangSelected_shouldReturnFalse_WhenLangDoesNotMatchTheConf(){
		// Given
		List list = new ArrayList<String>();
		list.add("en");
		list.add("fr");
		LogMapper logMapper = ((LogMapper)mapDriver.getMapper());
		logMapper.langagesSelection = list;
		CustomKey key = new CustomKey();
		key.setLang("ca");
		
		// Then
		boolean result = logMapper.isRecordLangSelected(key);
		
		// When
		Assert.assertThat(result, is(equalTo(false)));
	}
	

	
	@Test
	public void isRecordToBeIgnored_shouldReturnTrue_WhenPageNameDoesNotMatchTheConf(){
		// Given
		List list = new ArrayList<String>();
		list.add("Special");
		list.add("undefined");
		LogMapper logMapper = ((LogMapper)mapDriver.getMapper());
		logMapper.subjectsToIgnore = list;
		CustomKey key = new CustomKey();
		key.setPageName("Undefined");
		
		// Then
		boolean result = logMapper.isRecordToBeIgnored(key);
		
		// When
		Assert.assertThat(result, is(equalTo(true)));
	}
	
	@Test
	public void reduce_ShouldRetrieveTopTenViewed_WikipediaPage() throws IOException{
		// Given
		CustomKey keyIn = new CustomKey();
		keyIn.setLang("fr");
		keyIn.setMonth(5);
		keyIn.setYear(2014);
        keyIn.setDay(1);
		keyIn.setPageName("Tir_aux_Jeux_olympiques");
		LongWritable valueIn = new LongWritable(513L);
		List<LongWritable> valuesIn = new ArrayList<LongWritable>();
		valuesIn.add(valueIn);
		CustomKey keyOut = new CustomKey();
		keyOut.setLang("fr");
		keyOut.setMonth(5);
		keyOut.setYear(2014);
        keyOut.setDay(1);
        keyOut.setCount(513L);
		keyOut.setPageName("Tir_aux_Jeux_olympiques");
		LongWritable valueOut = new LongWritable(513L);
		reduceDriver.withInput(keyIn, valuesIn);
		reduceDriver.withOutput(keyOut, valueOut);
		
		// When
		reduceDriver.runTest();
		
		// Then
	}
}
