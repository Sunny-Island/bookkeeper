package org.apache.bookkeeper.bookie;

import com.intel.pmem.llpl.AnyHeap;
import com.intel.pmem.llpl.HeapException;
import com.intel.pmem.llpl.PersistentHeap;
import com.intel.pmem.llpl.PersistentMemoryBlock;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import lib.util.persistent.ObjectDirectory;
import lib.util.persistent.PersistentInteger;
import lib.util.persistent.PersistentLong;
import lib.util.persistent.PersistentString;


import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Pool<T> {
    public void push(T item) {
        items.add(item);
    }

    public T pop() {
        if (items.size() > 0) {
            return items.remove();
        } else {
            return null;
        }
    }

    public int size() {
        return items.size();
    }

    private LinkedList<T> items = new LinkedList<>();
}

public class PMemChannelProvider implements FileChannelProvider{
    @Override
    public BookieFileChannel open(File file, ServerConfiguration configuration) throws IOException {
        File heap_path = configuration.getJournalDirs()[0];
        if(PMemChannel.heap_inited==false){
            String pmemDir = heap_path.toString();
            File directory = new File(pmemDir);
            if (directory.exists()) {
                String[] entries = directory.list();
                for (String s : entries) {
                    File currentFile = new File(directory.getPath(), s);
                    currentFile.delete();
                }
                directory.delete();
            }
            directory.mkdirs();

            String path = pmemDir + "/heap";
            long size = 1024L * 1024 * 512 * 1;
            int initSize = 10 * 1024 * 1024;

            PMemChannel.initHeap(path, size, initSize, 0.9);
            PMemChannel.heap_inited = true;
        }
        return new PMemFileChannel(file);
    }
    public class PMemFileChannel implements BookieFileChannel{
        File file;
        int channelSize = 10 * 1024 * 1024;
        PMemFileChannel(File file){this.file = file;}
        @Override
        public FileChannel getFileChannel() throws IOException {
            return PMemChannel.openChannel(file.toPath(), channelSize,false);
        }

        @Override
        public boolean fileExists(File file) {
            PersistentLong handleP = ObjectDirectory.get(file.toString(), PersistentLong.class);
            return handleP != null;
        }

        @Override
        public FileDescriptor getFD() throws IOException {
            return null;
        }
    }
   static class PMemChannel extends FileChannel{

       private static final Logger LOG = LoggerFactory.getLogger(PMemChannel.class);
       private static int poolAllocatedSize = 0;
       private static int poolSize = 0;
       private static PersistentHeap heap;
       private static volatile boolean inited = false;
       public static volatile boolean heap_inited = false;
       private static Pool<PersistentMemoryBlock> blockPool = new Pool<>();
       private static AtomicInteger counter = new AtomicInteger();
       private static final Object GLOBAL_LOCK = new Object();

       private PersistentMemoryBlock pBlock;
       private long channelSize;
       private int channelPosition = 0;
       private Path filePath;
       private String sizeKey;

       /**
        * @param path heap path in PMem hardware.
        * @param size heap size to initiate.
        * @param allocatedSize size allocated to each block in pool
        * @param poolRatio pool size to heap size
        */
       public static void initHeap(String path, long size, int allocatedSize, double poolRatio) {
           String heapPath = path + "/pmem_heap";
           String metaPath = path + "/pmem_meta";

           File heapDir = new File(path);
           if (!heapDir.mkdirs()) {
               LOG.debug(heapDir + " already exists.");
           }

           try {
               BufferedWriter metaConfig = new BufferedWriter
                       (new OutputStreamWriter(new FileOutputStream("config.properties"), StandardCharsets.UTF_8));
               // entry number will be #poolBlock + #dynamicBlock (we assume its max is 100)
               long metaPoolSize = (poolSize + 100) * 1024L * 1024;
               String metaConfigContent = "path=" + metaPath + "\n" + "size=" + metaPoolSize + "\n";
               metaConfig.write(metaConfigContent);
               metaConfig.flush();
               LOG.info("Generate config.properties for pmdk pcj");

               metaConfig.close();
           } catch (IOException e) {
               e.printStackTrace();
           }

           if (!AnyHeap.exists(heapPath)) {
               if (poolRatio > 0.9) {
                   LOG.warn("poolRatio larger than 90%, may fail to allocate");
               }
               /**
                * poolSize: number of pBlock can used in current blockpool
                */
               int poolSize = (int) (size * poolRatio / allocatedSize);
               LOG.info("Init heap: size = " + size + ", poolSize = " + poolSize
                       + " (" + (poolRatio * 100) + "% of total size)" + ", poolEntry size = " + allocatedSize);
               ObjectDirectory.put("_heap_path", new PersistentString(heapPath));
               ObjectDirectory.put("_pool_allocated_size", new PersistentInteger(allocatedSize));
               ObjectDirectory.put("_pool_size", new PersistentInteger(poolSize));

               PersistentHeap heap = PersistentHeap.createHeap(heapPath, size);
               if (poolSize > 0) {
                   for (int i = 0; i < poolSize; i++) {
                       PersistentMemoryBlock block = heap.allocateMemoryBlock(allocatedSize);
                       // set data to 0
                       // block.setMemory(0, 0, allocatedSize)
                       ObjectDirectory.put("_pool_" + i, new PersistentLong(block.handle()));
                       LOG.info("init pool entry " + i);
                   }

                   // init pool used to 0
                   ObjectDirectory.put("_pool_used", new PersistentInteger(0));
               }
               LOG.info("init heap " + heapPath + " done");
           } else {
               LOG.info("PMem heap " + heapPath + " already exists. No need to re-init");
           }
       }

       /**
        *
        * @param file
        * @param initFileSize
        * @param preallocate
        * @return
        * @throws IOException
        */
       public static FileChannel openChannel(Path file, long initFileSize, boolean preallocate) throws IOException {
           synchronized (GLOBAL_LOCK) {
               LOG.info("open PMemChannel " + file.toString());

               if (!inited) {
                   inited = true;
                   heap = PersistentHeap.openHeap(ObjectDirectory.get("_heap_path", PersistentString.class).toString());
                   PersistentInteger poolAllocatedSizeP =
                           ObjectDirectory.get("_pool_allocated_size", PersistentInteger.class);
                   if (poolAllocatedSizeP != null) {
                       poolAllocatedSize = poolAllocatedSizeP.intValue();
                   } else {
                       poolAllocatedSize = 0;
                       LOG.error("PMem heap is not inited (poolAllocatedSize is not set)");
                   }

                   PersistentInteger poolSizeP = ObjectDirectory.get("_pool_size", PersistentInteger.class);
                   if (poolSizeP != null) {
                       poolSize = poolSizeP.intValue();
                   } else {
                       poolSize = 0;
                       LOG.error("PMem heap poolSize is not set");
                   }

                   counter.set(ObjectDirectory.get("_pool_used", PersistentInteger.class).intValue());

                   for (int i = 0; i < poolSize; i++) {
                       long handle = ObjectDirectory.get("_pool_" + i, PersistentLong.class).longValue();
                       PersistentString fileNameP = ObjectDirectory.get("_pool_handle_" + handle, PersistentString.class);
                       if (fileNameP == null || fileNameP.length() == 0) {
                           PersistentMemoryBlock block = heap.memoryBlockFromHandle(handle);
                           blockPool.push(block);
                       }
                   }
                   LOG.info("open heapPool with poolSize = " + poolSize + ", used = " + counter.get());
               }

               PMemChannel channel = null;
               try {
                   channel = new PMemChannel(file, initFileSize, preallocate);
               } catch (IOException e) {
                   LOG.error("Create PMemChannel exception: " + e);
               }
               return channel;
           }
       }

       public PMemChannel(Path file, long initSize, boolean preallocate) throws IOException {

           filePath = file;
           sizeKey = filePath.toString() + "/size";

           PersistentLong handleP = ObjectDirectory.get(file.toString(), PersistentLong.class);
           if (handleP != null) {  // already allocate, recover
               long handle = handleP.longValue();
               pBlock = heap.memoryBlockFromHandle(handle);
               if (initSize != 0) {
                   LOG.error("initSize not 0 for recovered channel. initSize = "
                           + initSize + ", buf.size = " + pBlock.size());
               }

               if (initSize != 0) {
                   channelSize = initSize;
                   ObjectDirectory.put(sizeKey, new PersistentLong(channelSize));
               } else {
                   // load the buffer size
                   PersistentInteger sizeP = ObjectDirectory.get(sizeKey, PersistentInteger.class);
                   if (sizeP != null) {
                       channelSize = sizeP.intValue();
                   } else {
                       channelSize = (int) pBlock.size();
                   }
               }

               LOG.info("recover block with handle " + handle);
           } else {  // allocate new block
               if (initSize == 0) {
                   LOG.error("PMemChannel initSize 0 (have to set log.preallocate=true)");
               }

               // TODO: what if initSize is 0
               if (poolSize == 0 || initSize != poolAllocatedSize || counter.get() >= poolSize) {
                   try {
                       pBlock = heap.allocateMemoryBlock(initSize);
                   } catch (HeapException e) {
                       LOG.error(e.toString());
                       throw e;
                   }
                   ObjectDirectory.put(file.toString(), new PersistentLong(pBlock.handle()));
                   LOG.info("Dynamically allocate " + initSize + " with handle " + pBlock.handle());
               } else {
                   if (counter.get() >= poolSize) {
                       LOG.error("PMem heap pool is full, currentUsed = " + counter.get() + ", poolSize = " + poolSize);
                       throw new IOException("PMem heap pool is full");
                   } else {
                       int usedCounter = counter.incrementAndGet();
                       ObjectDirectory.put("_pool_used", new PersistentInteger(usedCounter));
                       pBlock = blockPool.pop();
                       if (pBlock == null) {
                           String msg = "block pool inconsistent, usedCounter = "
                                   + usedCounter + "， poolSize = " + poolAllocatedSize;
                           LOG.error(msg);
                           throw new IOException(msg);
                       }
                       ObjectDirectory.put(file.toString(), new PersistentLong(pBlock.handle()));
                       ObjectDirectory.put("_pool_handle_" + pBlock.handle(), new PersistentString(file.toString()));
                       LOG.info("create new block " + file + " with handle " + pBlock.handle());
                   }
               }
               channelSize = initSize;
           }

           // create an empty log file as Kafka will check its existence
           if (!file.toFile().createNewFile()) {
               LOG.debug(file + " already exits");
           }
           LOG.info("Allocate PMemChannel with size " + channelSize);
       }

       @Override
       public int read(ByteBuffer dst) throws IOException {
           return read(dst,0);
       }

       @Override
       public long read(ByteBuffer[] dsts, int offset, int length) throws UnsupportedOperationException {
           String msg = "read(ByteBuffer[] dsts, int offset, int length) not implemented";
           LOG.error(msg);
           throw new UnsupportedOperationException(msg);
       }

       @Override
       public int write(ByteBuffer src) throws IOException {
           int writeSize = src.remaining();
           if (writeSize <= 0) {
               return writeSize;
           }

           int requiredSize = writeSize + channelPosition;

           // TODO (zhanghao): re-allocate
           if (requiredSize > channelSize) {
               if (requiredSize <= pBlock.size()) {
                   channelSize = (int) pBlock.size();
               } else {
                   LOG.error("requiredSize " + requiredSize + " > buf limit " + pBlock.size());
                   return 0;
               }
           }

           LOG.info("write " + writeSize + " to buf from position "
                   + channelPosition + ", size = " + size() + ", src.limit() = "
                   + src.limit() + ", src.position = " + src.position() + ", src.capacity() = " + src.capacity());
           for(int i = 0;i<writeSize;i++){
               pBlock.setByte(channelPosition++,src.get());
           }
           // _buf.flush(_position, writeSize);
           LOG.info("After write, final position = " + channelPosition);
           return writeSize;
       }

       @Override
       public long write(ByteBuffer[] srcs, int offset, int length) throws UnsupportedOperationException {
           String msg = "write(ByteBuffer[] srcs, int offset, int length) not implemented";
           LOG.error(msg);
           throw new UnsupportedOperationException(msg);
       }

       @Override
       public long position() throws IOException {
           LOG.debug("position = " + channelPosition);
           return channelPosition;
       }

       @Override
       public FileChannel position(long newPosition) throws IOException {
           LOG.debug("new position = " + newPosition + ", old position = " + channelPosition);
           channelPosition = (int) newPosition;
           return this;
       }

       @Override
       public long size() throws IOException {
           return channelSize;
       }

       @Override
       public FileChannel truncate(long size) throws IOException {
           LOG.info("PMemChannel truncate from " + this.channelSize + " to " + size);
           if (size <= pBlock.size()) {
               this.channelSize = (int) size;
               position(Math.min(position(), this.channelSize));
               synchronized (GLOBAL_LOCK) {
                   ObjectDirectory.put(sizeKey, new PersistentLong(this.channelSize));
               }
               return this;
           } else {
               String msg = "PMemChannel does not support truncate to larger size";
               LOG.error(msg);
               throw new IOException(msg);
           }
       }

       @Override
       public void force(boolean metaData) {
           // PersistentMemoryBlock do the sync automatically
       }

       @Override
       public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
           long transferSize = Math.min(channelSize - position, count);
           LOG.debug("transferTo @" + position + " with length " + count + ":" + transferSize);
           ByteBuffer transferBuf = pBlock.asByteBuffer(position, (int) count);
           int n = 0;
           while (n < transferSize) {
               n += target.write(transferBuf);
           }
           LOG.debug("write " + n + " bytes");
           return n;
       }

       @Override
       public long transferFrom(ReadableByteChannel src, long position, long count) throws UnsupportedOperationException {
           String msg = "transferFrom not implemented";
           LOG.error(msg);
           throw new UnsupportedOperationException(msg);
       }

       @Override
       public int read(ByteBuffer dst, long position) throws IOException {
           LOG.debug("dst.remaining() = " + dst.remaining() + ", size = " + channelSize
                   + ", position = " + position + ", curPos = " + this.channelPosition);
           int readSize = Math.min((int) channelSize - (int) position, dst.remaining());
           if (readSize <= 0) {
               return -1;
           }

           pBlock.copyToArray(position, dst.array(), dst.arrayOffset() + dst.position(), readSize);
           dst.position(dst.position() + readSize);
           LOG.debug("read " + readSize + " from position " + position);
           return readSize;
       }

       @Override
       public int write(ByteBuffer src, long position) throws UnsupportedOperationException {
           return 0;

       }

       @Override
       public MappedByteBuffer map(MapMode mode, long position, long size) throws UnsupportedOperationException {
           String msg = "map not implemented";
           LOG.error(msg);
           throw new UnsupportedOperationException(msg);
       }

       @Override
       public FileLock lock(long position, long size, boolean shared) throws UnsupportedOperationException {
           String msg = "lock not implemented";
           LOG.error(msg);
           throw new UnsupportedOperationException(msg);
       }

       @Override
       public FileLock tryLock(long position, long size, boolean shared) throws UnsupportedOperationException {
           String msg = "tryLock not implemented";
           LOG.error(msg);
           throw new UnsupportedOperationException(msg);
       }

       @Override
       protected void implCloseChannel() throws IOException {
       }

       public void delete() {
           synchronized (GLOBAL_LOCK) {
               LOG.info("Before delete PMemChannel, channelSize = " + pBlock.size() + ", poolSize = "
                       + blockPool.size() + ", usedCounter = " + counter.get());
               if (pBlock.size() == poolAllocatedSize) {
                   // clear the pmem metadata
                   ObjectDirectory.remove(filePath.toString(), PersistentLong.class);
                   ObjectDirectory.remove("_pool_handle_" + pBlock.handle(), PersistentString.class);
                   ObjectDirectory.remove(sizeKey, PersistentInteger.class);

                   // reset memory
                   // _buf.setMemory((byte)0, 0, poolAllocatedSize);
                   // push back to pool
                   blockPool.push(pBlock);
                   int usedCounter = counter.decrementAndGet();
                   ObjectDirectory.put("_pool_used", new PersistentInteger(usedCounter));

                   if (poolSize - usedCounter != blockPool.size()) {
                       LOG.error("pool free size (" + blockPool.size() + ") != poolSize - usedCounter ("
                               + (poolSize - usedCounter) + ")");
                   }
               } else {
                   pBlock.freeMemory();
               }
               LOG.info("After delete PMemChannel, channelSize = "
                       + pBlock.size() + ", poolSize = " + blockPool.size() + ", usedCounter = " + counter.get());
               pBlock = null;
           }
       }

       private String concatPath(String str) {
           return "[" + filePath + "]: " + str;
       }
//
//
//       @Override
//       public FileChannel getFileChannel() {
//           return this;
//       }
//
//       @Override
//       public boolean fileExists(File file) {
//           PersistentLong handleP = ObjectDirectory.get(file.toString(), PersistentLong.class);
//           return handleP != null;
//       }
//
//       @Override
//       public FileDescriptor getFD() throws IOException {
//           return null;
//       }
   }
}
