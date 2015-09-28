package com.hpe.caf.worker.datastore.fs;


import com.hpe.caf.api.HealthResult;
import com.hpe.caf.api.worker.DataStore;
import com.hpe.caf.api.worker.DataStoreException;
import com.hpe.caf.api.worker.DataStoreMetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * This is a simple DataStore that reads and writes files to and from
 * a directory upon the file system. The store directory must be an
 * absolute path.
 */
public class FileSystemDataStore extends DataStore
{
    private Path dataStorePath;
    private final AtomicInteger errors = new AtomicInteger(0);
    private final AtomicInteger numRx = new AtomicInteger(0);
    private final AtomicInteger numTx = new AtomicInteger(0);
    private final DataStoreMetricsReporter metrics = new FileSystemDataStoreMetricsReporter();
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemDataStore.class);


    /**
     * {@inheritDoc}
     *
     * Determine the directory for the data store, and create it if necessary.
     */
    public FileSystemDataStore(final FileSystemDataStoreConfiguration config)
            throws DataStoreException
    {
        dataStorePath = FileSystems.getDefault().getPath(config.getDataDir());
        if ( !Files.exists(dataStorePath) ) {
            try {
                Files.createDirectory(dataStorePath);
            } catch (IOException e) {
                throw new DataStoreException("Cannot create data store directory", e);
            }
        }
        LOG.debug("Initialised");
    }


    @Override
    public void shutdown()
    {
        // nothing to do
    }


    /**
     * {@inheritDoc}
     * Read a file from disk in the data directory.
     * @throws InvalidPathException if the reference cannot be converted to a Path
     */
    @Override
    public InputStream getInputStream(final String reference)
            throws DataStoreException
    {
        Objects.requireNonNull(reference);
        try {
            numRx.incrementAndGet();
            LOG.debug("Requesting {}", reference);
            return Files.newInputStream(verifyReference(reference));
        } catch (IOException e) {
            errors.incrementAndGet();
            throw new DataStoreException("Failed to retrieve data", e);
        }
    }


    /**
     * {@inheritDoc}
     * @throws InvalidPathException if the reference cannot be converted to a Path
     */
    @Override
    public long getDataSize(final String reference)
        throws DataStoreException
    {
        Objects.requireNonNull(reference);
        try {
            return Files.size(verifyReference(reference));
        } catch (IOException e) {
            errors.incrementAndGet();
            throw new DataStoreException("Failed to get data size", e);
        }
    }


    @Override
    public DataStoreMetricsReporter getMetrics()
    {
        return metrics;
    }


    /**
     * {@inheritDoc}
     * @throws InvalidPathException if the reference cannot be converted to a Path
     */
    @Override
    public OutputStream getOutputStream(final String reference)
        throws DataStoreException
    {
        try {
            return Files.newOutputStream(verifyReference(reference));
        } catch (IOException e) {
            errors.incrementAndGet();
            throw new DataStoreException("Failed to get output stream for store", e);
        }
    }


    /**
     * {@inheritDoc}
     * @throws InvalidPathException if the baseReference cannot be converted to a Path
     */
    @Override
    public String resolve(final String baseReference, final String reference)
    {
        return Paths.get(baseReference).resolve(reference).toString();
    }


    @Override
    public HealthResult healthCheck()
    {
        return HealthResult.RESULT_HEALTHY;
    }


    /**
     * Prevent a caller trying to "break out" of the root dataStorePath by performing
     * a full path resolution of the reference.
     * @param reference the data store reference relative to dataStorePath to resolve
     * @return the resolved path, if valid
     * @throws DataStoreException if the reference is invalid or cannot be resolved
     * @throws InvalidPathException if the reference cannot be converted to a Path
     */
    private Path verifyReference(final String reference)
        throws DataStoreException
    {
        Path p = dataStorePath.resolve(reference).normalize();
        if ( !p.startsWith(dataStorePath) ) {
            throw new DataStoreException("Invalid reference");
        }
        return p;
    }


    private class FileSystemDataStoreMetricsReporter implements DataStoreMetricsReporter
    {
        @Override
        public int getStoreRequests()
        {
            return numTx.get();
        }


        @Override
        public int getRetrieveRequests()
        {
            return numRx.get();
        }


        @Override
        public int getErrors()
        {
            return errors.get();
        }
    }
}
