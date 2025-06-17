function AzCopyOneLakeFiles {
    param (
        [string]$source,
        [string]$destination,
        [string]$ScratchDirectory
    )
    # AzCopy fails on files when doing direct copy from one lake to another
    # Current workaround is to stage locally first then copy to new onelake
    New-Item -ItemType Directory -Path $ScratchDirectory -Force | Out-Null
    azcopy copy --trusted-microsoft-suffixes=onelake.dfs.fabric.microsoft.com $source $ScratchDirectory --recursive
    azcopy copy --trusted-microsoft-suffixes=onelake.dfs.fabric.microsoft.com "$ScratchDirectory/*" $destination --recursive
    rm -rf $ScratchDirectory
}