import java.util.Date;

public class TickerInfo {

             String Ticker;
             String TransDate;
             double OpenPrice;
             double ClosePrice;
             
             public TickerInfo(String Ticker, String TransDate, double OpenPrice,double ClosePrice){
                this.Ticker = Ticker;
                this.TransDate = TransDate;
                this.OpenPrice = OpenPrice;
                this.ClosePrice = ClosePrice;       
             }
             
             
             
             public String getTicker(){
                return this.Ticker;
             }

	        public double getOpenPrice(){
	            return this.OpenPrice;
	        }

	        public double getClosePrice(){
                 return this.ClosePrice;	
	        }

            public String getTransDate(){
                 return this.TransDate;
            }
}
