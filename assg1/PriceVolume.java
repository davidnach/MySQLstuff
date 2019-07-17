import java.util.Date;

Public class TickerInfo {

             String Ticker;
             Date TransDate
             double OpenPrice;
             double HighPrice;
             double LowPrice;
             double ClosePrice;
             double Volume;
             double Adjusted Close;
             
             
             public TickerInfo(String Ticker, Date TransDate, double OpenPrice, double HighPrice,double LowPrice,
             double ClosePrice,double Volume,double AdjustedClose){
                this.Ticker = Ticker;
                this.TransDate = TransDate;
                this.OpenPrice = OpenPrice;
                this.HighPrice = HighPrice;
                this.LowPrice = LowPrice;
                this.ClosePrice = ClosePrice;
                this.Volume = Volume;
                this.AdjustedClose = AdjustedClose;             
             }
             
             
             
             public String getTicker(){
                return this.Ticker;
             }