import java.io.IOException;

public class Test {

	public static void main(String args[]){
		
		DataFetcher dataFetcher=new DataFetcher();
//		dataFetcher.print();
			
				try {
//					dataFetcher.downloadFile("http://www.metoffice.gov.uk/pub/data/weather/uk/climate/datasets/Tmax/date/UK.txt","UK","MAX Temp");
					
					dataFetcher.getTableData();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		
	}
	
}
