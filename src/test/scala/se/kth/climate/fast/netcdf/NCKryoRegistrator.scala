package se.kth.climate.fast.netcdf

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import ucar.nc2.NetcdfFile
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import ucar.nc2.iosp.netcdf3.N3outputStreamWriter;
import java.util.UUID;

class NCKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    println("Registering custom serializers");
    kryo.register(classOf[NetcdfFile], new NetcdfFileSerializer())
  }
}

class NetcdfFileSerializer extends Serializer[NetcdfFile] {
  override def write(kryo: Kryo, output: Output, ncfile: NetcdfFile) {
    //output.writeInt(object.getRGB());
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
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[NetcdfFile]): NetcdfFile = {
    //return new Color(input.readInt(), true);
    val len = input.readInt();
    val raw = new Array[Byte](len);
    input.read(raw);
    NetcdfFile.openInMemory(UUID.randomUUID().toString(), raw);
  }
}
