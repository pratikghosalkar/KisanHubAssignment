import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class DataFetcher {

	double  uk_max_temp_avg, england_max_temp_avg, scotland_max_temp_avg, wales_max_temp_avg, uk_min_temp_avg,
			england_min_temp_avg, scotland_min_temp_avg, wales_min_temp_avg, uk_mean_temp_avg, england_mean_temp_avg,
			scotland_mean_temp_avg, wales_mean_temp_avg, uk_rainfall_avg, england_rainfall_avg, scotland_rainfall_avg,
			wales_rainfall_avg, uk_sunshine_avg, england_sunshine_avg, scotland_sunshine_avg, wales_sunshine_avg;

	ArrayList<WeatherData> uk_max_temp_list, england_max_temp_list, scotland_max_temp_list, wales_max_temp_list, uk_min_temp_list,
	england_min_temp_list, scotland_min_temp_list, wales_min_temp_list, uk_mean_temp_list, england_mean_temp_list,
	scotland_mean_temp_list, wales_mean_temp_list, uk_rainfall_list, england_rainfall_list, scotland_rainfall_list,
	wales_rainfall_list, uk_sunshine_list, england_sunshine_list, scotland_sunshine_list, wales_sunshine_list;
	
	public void getTableData() {
		Document doc;
		System.out.println("Started");

		HashMap<String, ArrayList<ArrayList<WeatherData>>> weatherMap = new HashMap<>();
		try {
			doc = Jsoup.connect("https://www.metoffice.gov.uk/climate/uk/summaries/datasets#yearOrdered").get();

			Element table = doc.select("table.table").get(1);
			Elements rows = table.select("tr");

			for (int j = 1; j < 5; j++) {
				Elements cols = rows.get(j).select("td");

				if (cols != null && cols.size() > 0) {
					String title = "";
					ArrayList<ArrayList<WeatherData>> weatherDataList = new ArrayList<>();
					for (int i = 1; i < 6; i++) {
						// System.out.println(cols.get(i).select("a[href]").attr("href"));
						title = cols.get(i).select("a[href]").attr("title");
						String url = cols.get(i).select("a[href]").attr("href");
						String arr[] = title.split(" ");

						if (arr[2].equals(Constants.WEATHER_PARAMETER[0])) {
							ArrayList<WeatherData> tmaxList = downloadFile(url, arr[0],
									Constants.WEATHER_PARAMETER_FOR_CSV[0]);
							weatherDataList.add(tmaxList);
						} else if (arr[2].equals(Constants.WEATHER_PARAMETER[1])) {
							ArrayList<WeatherData> tminList = downloadFile(url, arr[0],
									Constants.WEATHER_PARAMETER_FOR_CSV[1]);
							weatherDataList.add(tminList);
						} else if (arr[2].equals(Constants.WEATHER_PARAMETER[2])) {
							ArrayList<WeatherData> tmeanList = downloadFile(url, arr[0],
									Constants.WEATHER_PARAMETER_FOR_CSV[2]);
							weatherDataList.add(tmeanList);
						} else if (arr[2].equals(Constants.WEATHER_PARAMETER[3])) {
							ArrayList<WeatherData> sunshineList = downloadFile(url, arr[0],
									Constants.WEATHER_PARAMETER_FOR_CSV[3]);
							weatherDataList.add(sunshineList);
						} else if (arr[2].equals(Constants.WEATHER_PARAMETER[4])) {
							ArrayList<WeatherData> rainfallList = downloadFile(url, arr[0],
									Constants.WEATHER_PARAMETER_FOR_CSV[4]);
							weatherDataList.add(rainfallList);
						}
					}
					weatherMap.put(title.split(" ")[0], weatherDataList);
				}
			}

			writeToCSV(weatherMap);
			
			double max_temp=max(uk_max_temp_avg, england_max_temp_avg,scotland_max_temp_avg,wales_max_temp_avg);
			if (max_temp==uk_max_temp_avg) {
				WeatherData data=getFactsData(uk_max_temp_list);
				
				System.out.println("Averagely "+data.region_code+" has the highest value for "+data.weather_param.toLowerCase()+" among other regions. From 1910 to 2017 it was highest in "+data.key.toLowerCase()+" "+data.year +" and value was "+data.value);
//				 System.out.println("In "+data.region_code+" region "+data.weather_param+" value in "+data.year+" is "+data.value);
			}else if (max_temp==england_max_temp_avg) {
				
			}else if (max_temp==wales_max_temp_avg) {
				
			}else if (max_temp==scotland_max_temp_avg) {
				
			}
				
			
			
			System.out.println("CSV File is ready");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * To Download the file from the given URL and create a arraylist of all
	 * files
	 */
	public ArrayList<WeatherData> downloadFile(String url, String title, String weatherParam) throws IOException {
		ArrayList<WeatherData> weatherDataList = new ArrayList<>();

		BufferedReader bufferedReader;
		try {
			url = url.replace("http", "https");
			URL fileUrl = new URL(url);
			String line = null;

			URLConnection con = fileUrl.openConnection();
			InputStream inputStream = con.getInputStream();
			InputStreamReader streamReader = new InputStreamReader(inputStream);
			bufferedReader = new BufferedReader(streamReader);

			// To skip first 8 lines of description
			for (int i = 1; i <= 8; i++) {
				bufferedReader.readLine();
			}

			while ((line = bufferedReader.readLine()) != null) {
				line = line.replaceAll("\\s{5,}", " " + Constants.NA + " ").trim();
				line = line.replaceAll("\\s+", ",");

				String[] arr = line.split(",");

				for (int i = 0, j = 1; i < Constants.MONTHS.length; i++, j++) {
					weatherDataList.add(new WeatherData(title, weatherParam, arr[0], Constants.MONTHS[i], arr[j]));
				}

			}
			inputStream.close();
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
		}

		return weatherDataList;
	}

	private void writeToCSV(HashMap<String, ArrayList<ArrayList<WeatherData>>> weatherMap) {
		File downloadDirectory = new File("download directory");
		if (!downloadDirectory.exists()) {
			downloadDirectory.mkdirs();
		}

		BufferedReader bufferedReader;
		FileWriter writer = null;
		try {
			writer = new FileWriter(downloadDirectory + "/" + "weather_data.csv");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Delimiter used in CSV file
		String COMMA_DELIMITER = ",";
		String NEW_LINE_SEPARATOR = "\n";
		// CSV file header
		String FILE_HEADER = "region_code,weather_param,year, key, value";

		// Write the CSV file header
		try {
			writer.append(FILE_HEADER.toString());
			// Add a new line separator after the header
			writer.append(NEW_LINE_SEPARATOR);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int i = 0; i < Constants.REGION_LIST.length; i++) {
			ArrayList<ArrayList<WeatherData>> list = weatherMap.get(Constants.REGION_LIST[i]);
			Iterator it = list.iterator();
			while (it.hasNext()) {
				ArrayList<WeatherData> dataList = (ArrayList<WeatherData>) it.next();
				try {
					for (WeatherData weatherData : dataList) {
						writer.append(weatherData.getRegion_code());
						writer.append(COMMA_DELIMITER);
						writer.append(weatherData.getWeather_param());
						writer.append(COMMA_DELIMITER);
						writer.append(weatherData.getYear());
						writer.append(COMMA_DELIMITER);
						writer.append(weatherData.getKey());
						writer.append(COMMA_DELIMITER);
						writer.append(String.valueOf(weatherData.getValue()));
						writer.append(NEW_LINE_SEPARATOR);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
//				getFacts(dataList);
				getAverage(dataList);
			}
		}

		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void getFacts(ArrayList<WeatherData> dataList) {
		WeatherData data = Collections.max(dataList);

		// System.out.println("In "+data.region_code+" region
		// "+data.weather_param+" value in "+data.year+" is "+data.value);
		getAverage(dataList);
	}

	private void getAverage(ArrayList<WeatherData> dataList) {
		double avg = 0;
		for (WeatherData wd : dataList) {
			if (!wd.value.equals(Constants.NA))
				avg += Double.parseDouble(wd.value);
		}

//		System.out.println("Average " + dataList.get(0).weather_param + " for " + dataList.get(0).region_code
//				+ " region is " + (avg / dataList.size()));

		avg=avg/dataList.size();
		
		switch (dataList.get(0).region_code) {

		case "UK":
			switch (dataList.get(0).weather_param) {
			case "Max Temp":
				uk_max_temp_avg = avg;
				uk_max_temp_list=dataList;
				break;
			case "Min Temp":
				uk_min_temp_avg=avg;
				uk_min_temp_list=dataList;
				break;
			case "Mean Temp":
				uk_mean_temp_avg=avg;
				uk_mean_temp_list=dataList;
				break;
			case "Sunshine":
				uk_sunshine_avg=avg;
				uk_sunshine_list=dataList;
				break;
			case "Rainfall":
				uk_rainfall_avg=avg;
				uk_rainfall_list=dataList;
				break;
			}
			break;
		case "England":
			switch (dataList.get(0).weather_param) {
			case "Tmax":
				england_max_temp_avg = avg;
				england_max_temp_list=dataList;
				break;
			case "Tmin":
				england_min_temp_avg=avg;
				england_min_temp_list=dataList;
				break;
			case "Tmean":
				england_mean_temp_avg=avg;
				england_mean_temp_list=dataList;
				break;
			case "Sunshine":
				england_sunshine_avg=avg;
				england_sunshine_list=dataList;
				break;
			case "Rainfall":
				england_rainfall_avg=avg;
				england_rainfall_list=dataList;
				break;
			}
			break;
		case "Wales":
			switch (dataList.get(0).weather_param) {
			case "Tmax":
				wales_max_temp_avg = avg;
				wales_max_temp_list=dataList;
				break;
			case "Tmin":
				wales_min_temp_avg=avg;
				wales_min_temp_list=dataList;
				break;
			case "Tmean":
				wales_mean_temp_avg=avg;
				wales_mean_temp_list=dataList;
				break;
			case "Sunshine":
				wales_sunshine_avg=avg;
				wales_sunshine_list=dataList;
				break;
			case "Rainfall":
				wales_rainfall_avg=avg;
				wales_rainfall_list=dataList;
				break;
			}
			break;
		case "Scotland":
			switch (dataList.get(0).weather_param) {
			case "Tmax":
				scotland_max_temp_avg = avg;
				scotland_max_temp_list=dataList;
				break;
			case "Tmin":
				scotland_min_temp_avg=avg;
				scotland_min_temp_list=dataList;
				break;
			case "Tmean":
				scotland_mean_temp_avg=avg;
				scotland_mean_temp_list=dataList;
				break;
			case "Sunshine":
				scotland_sunshine_avg=avg;
				scotland_sunshine_list=dataList;
				break;
			case "Rainfall":
				scotland_rainfall_avg=avg;
				scotland_rainfall_list=dataList;
				break;
			}
			break;
		}
	}
	
	public static Double max(Double first, Double... rest) {
		Double ret = first;
	    for (Double val : rest) {
	        ret = Math.max(ret, val);
	    }
	    return ret;
	}
	
	private WeatherData getFactsData(ArrayList<WeatherData> dataList){
		WeatherData weatherData = null;
		Double prev=0.0;
		
		Iterator iterator=dataList.iterator();
		while (iterator.hasNext()) {
			WeatherData object = (WeatherData) iterator.next();
			Double curr=0.0;
			if (!object.value.equals(Constants.NA)) {
				curr=Double.parseDouble(object.value);
			}
			if (curr>0.0 && prev<curr) {
				prev=curr;
				weatherData=object;
			}
			
		}
		
		return weatherData;
	}
}
