package net.bluemud.tcp;

/**
 */
public class Pair<K, V> {

	public static <K, V> Pair<K, V> of (K key, V value) {
		return new Pair<K, V>(key, value);
	}

	private final K key;
	private final V value;

	private Pair(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}
}
