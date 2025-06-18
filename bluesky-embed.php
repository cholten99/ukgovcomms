<?php
$context = stream_context_create([
    "http" => [
        "method" => "GET",
        "header" => "User-Agent: Mozilla/5.0\r\n"
    ]
]);

$search_url = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts?q=%23ukgovcomms&limit=1";
$search_response = file_get_contents($search_url, false, $context);

if ($search_response === FALSE) {
    echo "<p>Could not fetch Bluesky post.</p>";
    return;
}

$search_data = json_decode($search_response, true);
if (!isset($search_data['posts'][0]['uri'])) {
    echo "<p>No matching posts found.</p>";
    return;
}

$post_uri = $search_data['posts'][0]['uri'];

$oembed_url = "https://embed.bsky.app/oembed?url=" . urlencode($post_uri);
$oembed_response = file_get_contents($oembed_url, false, $context);

if ($oembed_response === FALSE) {
    echo "<p>Could not fetch embed info.</p>";
    return;
}

$oembed_data = json_decode($oembed_response, true);
if (!isset($oembed_data['html'])) {
    echo "<p>Embed HTML not available.</p>";
    return;
}

echo $oembed_data['html'];
?>

