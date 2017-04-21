package se.kth.climate.fast.netcdf

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.{ Kryo, KryoException, Serializer }
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import ucar.nc2.NetcdfFile
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import ucar.nc2.iosp.netcdf3.N3outputStreamWriter;
import java.util.UUID;

class NCKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    println("Registering custom NetCDF serializers");
    com.esotericsoftware.minlog.Log.TRACE();
    kryo.register(classOf[NetcdfFile], new NetcdfFileSerializer());
    kryo.register(classOf[Array[String]], new com.esotericsoftware.kryo.serializers.DefaultArraySerializers.StringArraySerializer())
    kryo.register(classOf[Array[Int]], new com.esotericsoftware.kryo.serializers.DefaultArraySerializers.IntArraySerializer())

    //kryo.setRegistrationRequired(true);
  }
}

class NetcdfFileSerializer extends Serializer[NetcdfFile] {
  override def write(kryo: Kryo, output: Output, ncfile: NetcdfFile) {
    val baos = new ByteArrayOutputStream();
    val out = new DataOutputStream(baos);
    val w = new N3outputStreamWriter(ncfile);
    val ulim = ncfile.getUnlimitedDimension();
    val numrec = if (ulim == null) 0 else ncfile.getUnlimitedDimension().getLength();
    w.writeHeader(out, numrec);
    w.writeDataAll(out);
    out.flush();
    out.close();
    val byteArray = baos.toByteArray();
    //    println("**********BYTES***********");
    //    println(byteArray.take(100000).mkString);
    output.writeInt(byteArray.length);
    output.write(byteArray);
    println(s"******** Wrote ncfile (size=${byteArray.length}) **********");
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[NetcdfFile]): NetcdfFile = {
    val len = input.readInt();
    println(s"******** Reading ncfile (size=${len}) **********");
    val raw = new Array[Byte](len);
    var readBytes = 0;
    do {
      val res = input.read(raw, readBytes, len - readBytes);
      if (res > -1) {
        readBytes += res;
      } else {
        throw new KryoException(s"Read only $readBytes bytes when $len bytes were expected!");
      }
    } while (readBytes < len);
    println(s"******** Read ncfile (read=${readBytes}, size=${len}) **********");
    NetcdfFile.openInMemory(UUID.randomUUID().toString(), raw);
  }
}
