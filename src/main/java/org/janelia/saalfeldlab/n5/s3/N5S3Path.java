/**
 * Copyright (c) 2017--2020, Stephan Saalfeld
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.s3;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Minimum {@link Path} implementation for N5 on AWS S3
 *
 * @author Stephan Saalfeld
 */
public class N5S3Path implements Path {

	/* TODO put into FileSystem and make Path inner class */
	private static final String SEPARATOR = "/";

	private final N5S3FileSystem fileSystem;

	private final String[] nodes;

	/**
	 * Expands the nodes of a path that is provided in multiple pieces.
	 * If the path begins with "/", the first element will be "".
	 *
	 * @param nodes
	 * @return
	 */
	private static String[] expand(final String... nodes) {

		if (nodes == null || nodes.length == 0 || nodes.length == 1 && nodes[0].isEmpty()) return null;

		final ArrayList<String> expanded = new ArrayList<>();
		final String[] firstNodes = nodes[0].split(SEPARATOR, -1);
		expanded.add(firstNodes[0]);
		for (int i = 1; i < firstNodes.length; ++i) {
			final String node = firstNodes[i];
			if (!node.isEmpty()) expanded.add(node);
		}
		for (int i = 1; i < nodes.length; ++i)
			for (final String node : nodes[i].split(SEPARATOR))
				if (!node.isEmpty()) expanded.add(node);

		return expanded.toArray(new String[expanded.size()]);
	}

	public N5S3Path(final N5S3FileSystem fileSystem, final String... nodes) {

		this.fileSystem = fileSystem;
		this.nodes = expand(nodes);
	}

	@Override
	public N5S3FileSystem getFileSystem() {

		return fileSystem;
	}

	@Override
	public boolean isAbsolute() {

		return nodes != null && nodes[0].isEmpty();
	}

	@Override
	public Path getRoot() {

		return isAbsolute() ? new N5S3Path(fileSystem, "/") : null;
	}

	@Override
	public Path getFileName() {

		return nodes == null ? null : new N5S3Path(fileSystem, nodes[nodes.length - 1]);
	}

	@Override
	public Path getParent() {

		return nodes == null ? null : new N5S3Path(fileSystem, Arrays.copyOf(nodes, nodes.length - 1));
	}

	@Override
	public int getNameCount() {

		return nodes == null ? 0 : isAbsolute() ? nodes.length - 1 : nodes.length;
	}

	@Override
	public Path getName(final int index) {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path subpath(final int beginIndex, final int endIndex) {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean startsWith(final Path other) {

		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean endsWith(final Path other) {

		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Path normalize() {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path resolve(final Path other) {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path relativize(final Path other) {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URI toUri() {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path toAbsolutePath() {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path toRealPath(final LinkOption... options) throws IOException {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WatchKey register(final WatchService watcher, final Kind<?>[] events, final Modifier... modifiers) throws IOException {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int compareTo(final Path other) {

		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean startsWith(final String other) {

		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean endsWith(final String other) {

		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Path resolve(final String other) {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path resolveSibling(final Path other) {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path resolveSibling(final String other) {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public File toFile() {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WatchKey register(final WatchService watcher, final Kind<?>... events) throws IOException {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Path> iterator() {

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toString() {

		return nodes == null ? "" : String.join(SEPARATOR, nodes);
	}
}
