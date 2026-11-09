
import "dotenv/config";
import axios from "axios";
import { MongoClient } from "mongodb";

// ============================================================
// CONFIGURATION
// ============================================================

const TA_URL = process.env.TUBEARCHIVIST_URL;
const TA_API_KEY = process.env.TUBEARCHIVIST_API_KEY;

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = process.env.MONGODB_DATABASE || "tubearchivist";

const PAGE_SIZE = Number(
    process.env.TUBEARCHIVIST_PAGE_SIZE || 200
);

// Dry-run enabled by default.
const DRY_RUN =
    process.env.TUBEARCHIVIST_RESET_DRY_RUN !== "false";

// ============================================================
// LOGGER / HELPERS
// ============================================================

function log(message, level = "INFO") {
    console.log(
        `${new Date().toISOString()} [${level}] ${message}`
    );
}

function getErrorMessage(error) {
    return (
        error.response?.data?.detail ||
        error.response?.data?.error ||
        error.response?.data?.message ||
        error.message
    );
}

// ============================================================
// TUBEARCHIVIST API
// ============================================================

const taApi = axios.create({
    baseURL: TA_URL ? TA_URL.replace(/\/+$/, "") : undefined,
    timeout: 60000,
    headers: {
        Authorization: `Token ${TA_API_KEY || ""}`,
        Accept: "application/json",
        "Content-Type": "application/json"
    }
});

// ============================================================
// MONGODB
// ============================================================

let mongoClient;
let inventory;
let syncState;

async function connectDatabase() {

    mongoClient = new MongoClient(MONGODB_URI);

    await mongoClient.connect();

    const db = mongoClient.db(DB_NAME);

    inventory = db.collection("video_inventory");
    syncState = db.collection("sync_state");

    await inventory.createIndex(
        { youtubeId: 1 },
        { unique: true }
    );

    await inventory.createIndex(
        { "tubeArchivist.present": 1 }
    );

    await inventory.createIndex(
        { "sync.tubeArchivistResetStatus": 1 }
    );

    log(
        `MongoDB connected: ${DB_NAME}.video_inventory`
    );
}

// ============================================================
// GET COMPLETE TUBEARCHIVIST INVENTORY
// PAGINATION
// ============================================================

async function getAllTubeArchivistVideos() {

    const allVideos = [];

    let page = 0;
    let lastPage = null;
    let maxHits = false;

    while (true) {

        log(`Fetching TubeArchivist page ${page}`);

        let response;

        try {

            response = await taApi.get("/api/video/", {
                params: {
                    page,
                    page_size: PAGE_SIZE
                }
            });

        } catch (error) {

            if (
                error.response?.status === 404 &&
                page > 0
            ) {
                break;
            }

            throw error;
        }

        const result = response.data || {};

        const videos = Array.isArray(result.data)
            ? result.data
            : [];

        const pagination = result.paginate || {};

        allVideos.push(...videos);

        if (pagination.max_hits === true) {
            maxHits = true;
        }

        if (
            lastPage === null &&
            Number.isInteger(pagination.last_page)
        ) {
            lastPage = pagination.last_page;
        }

        log(
            `Page ${page}: ${videos.length} videos; collected ${allVideos.length}`
        );

        if (videos.length === 0) break;

        if (
            lastPage !== null &&
            page >= lastPage
        ) {
            break;
        }

        if (
            lastPage === null &&
            videos.length < PAGE_SIZE
        ) {
            break;
        }

        page++;
    }

    if (maxHits) {
        throw new Error(
            "TubeArchivist API reports max_hits. Refusing to reconcile an incomplete inventory."
        );
    }

    log(
        `Total TubeArchivist videos: ${allVideos.length}`
    );

    return allVideos;
}

// ============================================================
// NORMALIZE API VIDEO
// ============================================================

function normalizeVideo(video) {

    const youtubeId =
        video.youtube_id ||
        video.id;

    if (
        typeof youtubeId !== "string" ||
        !/^[A-Za-z0-9_-]{11}$/.test(youtubeId)
    ) {
        return null;
    }

    return {

        youtubeId,

        title: video.title || "",

        channelId:
            video.channel?.channel_id ||
            video.channel_id ||
            null,

        channelName:
            video.channel?.channel_name ||
            video.channel_name ||
            null,

        dateDownloaded:
            video.date_downloaded ?? null,

        watched:
            video.player?.watched === true ||
            video.watched === true ||
            video.is_watched === true,

        duration:
            video.player?.duration ||
            video.duration ||
            null,

        thumbnail:
            video.thumbnail_url ||
            video.thumbnail ||
            null
    };
}

// ============================================================
// UPSERT VIDEO INVENTORY
// ============================================================

async function upsertVideo(
    video,
    runId,
    baselineComplete
) {

    const now = new Date();

    const existing = await inventory.findOne({
        youtubeId: video.youtubeId
    });

    // ========================================================
    // NEW VIDEO
    // ========================================================

    if (!existing) {

        const initialBaseline = !baselineComplete;

        await inventory.insertOne({

            youtubeId: video.youtubeId,

            title: video.title,
            channelId: video.channelId,
            channelName: video.channelName,

            dateDownloaded: video.dateDownloaded,

            duration: video.duration,
            thumbnail: video.thumbnail,

            tubeArchivist: {
                present: true,
                watched: video.watched,
                lastSeen: now
            },

            jellyfin: {
                present: false,
                itemIds: []
            },

            downloadVersion: 1,

            sync: {

                jellyfinResetStatus:
                    initialBaseline
                        ? "not_required"
                        : "pending",

                tubeArchivistResetStatus:
                    initialBaseline
                        ? "not_required"
                        : "pending",

                jellyfinResetUsers: [],

                jellyfinRetryCount: 0,

                tubeArchivistResetRetryCount: 0
            },

            lastInventoryRunId: runId,

            createdAt: now,
            updatedAt: now

        });

        log(
            `NEW VIDEO: ${video.youtubeId} (${initialBaseline ? "baseline" : "queued for reset"})`
        );

        return "inserted";
    }

    // ========================================================
    // PREVIOUSLY MISSING VIDEO RETURNED
    // ========================================================

    const returned =
        existing.tubeArchivist?.present === false;

    if (returned) {

        const nextVersion =
            Number(existing.downloadVersion || 1) + 1;

        await inventory.updateOne(
            { youtubeId: video.youtubeId },
            {
                $set: {

                    title: video.title,
                    channelId: video.channelId,
                    channelName: video.channelName,

                    dateDownloaded: video.dateDownloaded,

                    duration: video.duration,
                    thumbnail: video.thumbnail,

                    "tubeArchivist.present": true,
                    "tubeArchivist.watched": video.watched,
                    "tubeArchivist.lastSeen": now,

                    // Queue both applications.

                    "sync.jellyfinResetStatus": "pending",

                    "sync.tubeArchivistResetStatus": "pending",

                    "sync.jellyfinResetUsers": [],

                    "sync.jellyfinRetryCount": 0,

                    "sync.tubeArchivistResetRetryCount": 0,

                    lastInventoryRunId: runId,

                    updatedAt: now
                },

                $inc: {
                    downloadVersion: 1
                },

                $unset: {

                    "sync.jellyfinResetVersion": "",
                    "sync.jellyfinResetAt": "",

                    "sync.tubeArchivistResetAt": "",

                    "sync.tubeArchivistLastError": "",

                    "sync.watchedResetStatus": ""
                }
            }
        );

        log(
            `RETURNED VIDEO: ${video.youtubeId}; version ${nextVersion}`,
            "WARN"
        );

        return "returned";
    }

    // ========================================================
    // EXISTING VIDEO
    // ========================================================

    await inventory.updateOne(
        { youtubeId: video.youtubeId },
        {
            $set: {

                title: video.title,
                channelId: video.channelId,
                channelName: video.channelName,

                dateDownloaded: video.dateDownloaded,

                duration: video.duration,
                thumbnail: video.thumbnail,

                "tubeArchivist.present": true,
                "tubeArchivist.watched": video.watched,
                "tubeArchivist.lastSeen": now,

                lastInventoryRunId: runId,

                updatedAt: now
            }
        }
    );

    return "updated";
}

// ============================================================
// MARK MISSING VIDEOS
// ONLY AFTER SUCCESSFUL INVENTORY SCAN
// ============================================================

async function markMissingVideos(runId) {

    const result = await inventory.updateMany(
        {
            "tubeArchivist.present": true,

            lastInventoryRunId: {
                $ne: runId
            }
        },
        {
            $set: {
                "tubeArchivist.present": false,
                updatedAt: new Date()
            }
        }
    );

    log(
        `Marked ${result.modifiedCount} videos as missing from TubeArchivist`
    );
}

// ============================================================
// PROCESS PENDING TUBEARCHIVIST WATCHED RESETS
// ============================================================

async function processPendingTubeArchivistResets() {

    log(
        "Checking pending TubeArchivist watched resets"
    );

    const pendingVideos = await inventory.find({

        "tubeArchivist.present": true,

        "sync.tubeArchivistResetStatus": "pending"

    }).toArray();

    const summary = {

        total: pendingVideos.length,

        success: 0,

        failed: 0,

        dryRun: 0
    };

    log(
        `Pending TubeArchivist resets: ${pendingVideos.length}`
    );

    for (const video of pendingVideos) {

        const youtubeId = video.youtubeId;

        try {

            // DRY RUN

            if (DRY_RUN) {

                log(
                    `[DRY RUN] Would mark TubeArchivist unwatched: ${youtubeId}`
                );

                summary.dryRun++;

                continue;
            }

            // ACTUAL TUBEARCHIVIST API REQUEST

            await taApi.post("/api/watched/", {

                id: youtubeId,

                is_watched: false

            });

            // UPDATE MONGODB AFTER API SUCCESS

            await inventory.updateOne(
                {
                    youtubeId,

                    "sync.tubeArchivistResetStatus": "pending"
                },
                {
                    $set: {

                        "tubeArchivist.watched": false,

                        "sync.tubeArchivistResetStatus": "completed",

                        "sync.tubeArchivistResetAt": new Date(),

                        "sync.tubeArchivistLastAttempt": new Date(),

                        updatedAt: new Date()
                    },

                    $unset: {
                        "sync.tubeArchivistLastError": ""
                    }
                }
            );

            summary.success++;

            log(
                `SUCCESS: ${youtubeId} marked unwatched in TubeArchivist`
            );

        } catch (error) {

            summary.failed++;

            const message = getErrorMessage(error);

            await inventory.updateOne(
                { youtubeId },
                {
                    $set: {

                        "sync.tubeArchivistResetStatus": "pending",

                        "sync.tubeArchivistLastError": message,

                        "sync.tubeArchivistLastAttempt": new Date(),

                        updatedAt: new Date()
                    },

                    $inc: {

                        "sync.tubeArchivistResetRetryCount": 1

                    }
                }
            );

            log(
                `FAILED: ${youtubeId}: ${message}`,
                "ERROR"
            );
        }
    }

    log("--------------------------------");
    log("TUBEARCHIVIST RESET SUMMARY");
    log(`Total: ${summary.total}`);
    log(`Success: ${summary.success}`);
    log(`Dry run: ${summary.dryRun}`);
    log(`Failed: ${summary.failed}`);
    log("--------------------------------");

    return summary;
}

// ============================================================
// MAIN INVENTORY SYNC
// ============================================================

async function syncTubeArchivistInventory() {

    const runId = new Date().toISOString();

    const summary = {

        total: 0,
        inserted: 0,
        updated: 0,
        returned: 0,
        failed: 0
    };

    log(
        "Starting TubeArchivist inventory sync"
    );

    // CHECK INITIAL BASELINE

    const state = await syncState.findOne({
        _id: "tubeArchivistInventory"
    });

    const existingInventoryCount =
        await inventory.countDocuments();

    const baselineComplete =
        state?.baselineComplete === true ||
        existingInventoryCount > 0;

    log(
        `Initial baseline complete: ${baselineComplete}`
    );

    // FETCH API INVENTORY

    const videos = await getAllTubeArchivistVideos();

    summary.total = videos.length;

    if (videos.length === 0) {

        throw new Error(
            "TubeArchivist returned zero videos. Aborting reconciliation."
        );
    }

    // PROCESS VIDEO RECORDS

    for (const rawVideo of videos) {

        const video = normalizeVideo(rawVideo);

        if (!video) {

            summary.failed++;

            log(
                "Skipping API item without a valid 11-character YouTube ID",
                "ERROR"
            );

            continue;
        }

        try {

            const result = await upsertVideo(
                video,
                runId,
                baselineComplete
            );

            if (result === "inserted") {

                summary.inserted++;

            } else if (result === "returned") {

                summary.returned++;

            } else {

                summary.updated++;

            }

        } catch (error) {

            summary.failed++;

            log(
                `Failed ${video.youtubeId}: ${getErrorMessage(error)}`,
                "ERROR"
            );
        }
    }

    // ========================================================
    // RECONCILIATION
    // ========================================================

    if (summary.failed > 0) {

        log(
            "Reconciliation and reset queue skipped because inventory had errors",
            "WARN"
        );

    } else {

        await markMissingVideos(runId);

        // SAVE BASELINE STATE

        await syncState.updateOne(
            {
                _id: "tubeArchivistInventory"
            },
            {
                $set: {

                    baselineComplete: true,

                    lastSuccessfulRun: new Date(),

                    lastInventoryRunId: runId

                }
            },
            {
                upsert: true
            }
        );

        // PROCESS PENDING WATCHED RESETS

        const resetSummary =
            await processPendingTubeArchivistResets();

        log(
            `TubeArchivist reset results: ${resetSummary.success} successful, ${resetSummary.dryRun} dry-run, ${resetSummary.failed} failed`
        );
    }

    // ========================================================
    // FINAL SUMMARY
    // ========================================================

    log("--------------------------------");
    log("INVENTORY SYNC SUMMARY");

    log(`Total: ${summary.total}`);
    log(`Inserted: ${summary.inserted}`);
    log(`Updated: ${summary.updated}`);
    log(`Returned: ${summary.returned}`);
    log(`Failed: ${summary.failed}`);

    log("--------------------------------");

    return summary;
}

// ============================================================
// EXECUTION
// ============================================================

async function main() {

    try {

        if (
            !TA_URL ||
            !TA_API_KEY ||
            !MONGODB_URI
        ) {

            throw new Error(
                "Missing TUBEARCHIVIST_URL, TUBEARCHIVIST_API_KEY or MONGODB_URI"
            );
        }

        await connectDatabase();

        await syncTubeArchivistInventory();
console.log('completed');

    } catch (error) {

        log(
            getErrorMessage(error),
            "FATAL"
        );

        process.exitCode = 1;

    } finally {

        if (mongoClient) {

            await mongoClient.close();

        }
    }
}

main();
