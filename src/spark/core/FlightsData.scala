package spark.core

import org.apache.hadoop.io.Writable

case class FlightsData(
  var date : String = "",
  var departureTime : String = "",
  var departureTimeCRS : String = "",
  var arrivalTime : String = "",
  var arrivalTimeCRS : String = "",
  var uniqueCarrier : String = "",
  var flightNumber : String = "",
  var tailNumber : String = "",
  var elapsedTime : Int = 0,
  var elapsedTimeCRS : Int = 0,
  var airTime : Int = 0,
  var arrivalDelay : Int = 0,
  var departureDelay : Int = 0,
  var origin : String = "",
  var destination : String = "",
  var distance : Int = 0,
  var taxiIn : Int = 0,
  var taxiOut : Int = 0,
  var cancelled : String = "",
  var cancellationCode : String = "",
  var diverted : String = "",
  var carrierDelay : Int = 0,
  var weatherDelay : Int = 0,
  var nasDelay : Int = 0,
  var securityDelay : Int = 0,
  var lateAircraftDelay : Int = 0
) extends Writable {
  
  def this() = {
    this("","","","","","","","", 0,0,0,0,0, "", "", 0,0,0,"","","",0,0,0,0,0)
  }
  
  def readFields(in: java.io.DataInput): Unit = {
    date = in.readUTF()
    departureTime = in.readUTF()
    departureTimeCRS = in.readUTF()
    arrivalTime = in.readUTF()
    arrivalTimeCRS = in.readUTF()
    uniqueCarrier = in.readUTF()
    flightNumber = in.readUTF()
    tailNumber = in.readUTF()
    elapsedTime = in.readInt()
    elapsedTimeCRS = in.readInt()
    airTime = in.readInt()
    arrivalDelay = in.readInt()
    departureDelay = in.readInt()
    origin = in.readUTF()
    destination = in.readUTF()
    distance = in.readInt()
    taxiIn = in.readInt()
    taxiOut = in.readInt()
    cancelled = in.readUTF()
    cancellationCode = in.readUTF()
    diverted = in.readUTF()
    carrierDelay = in.readInt()
    weatherDelay = in.readInt()
    nasDelay = in.readInt()
    securityDelay = in.readInt()
    lateAircraftDelay = in.readInt()
  }
  
  def write(out: java.io.DataOutput): Unit = {
    out.writeUTF(date)
    out.writeUTF(departureTime)
    out.writeUTF(departureTimeCRS)
    out.writeUTF(arrivalTime)
    out.writeUTF(arrivalTimeCRS)
    out.writeUTF(uniqueCarrier)
    out.writeUTF(flightNumber)
    out.writeUTF(tailNumber)
    out.writeInt(elapsedTime)
    out.writeInt(elapsedTimeCRS)
    out.writeInt(airTime)
    out.writeInt(arrivalDelay)
    out.writeInt(departureDelay)
    out.writeUTF(origin)
    out.writeUTF(destination)
    out.writeInt(distance)
    out.writeInt(taxiIn)
    out.writeInt(taxiOut)
    out.writeUTF(cancelled)
    out.writeUTF(cancellationCode)
    out.writeUTF(diverted)
    out.writeInt(carrierDelay)
    out.writeInt(weatherDelay)
    out.writeInt(nasDelay)
    out.writeInt(securityDelay)
    out.writeInt(lateAircraftDelay)
  }
}